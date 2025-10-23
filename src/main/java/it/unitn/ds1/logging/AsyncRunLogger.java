package it.unitn.ds1.logging;

import it.unitn.ds1.logging.LogModels.Event;
import it.unitn.ds1.logging.LogModels.Summary;
import it.unitn.ds1.logging.LogModels.Metadata;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardOpenOption.*;

/**
 *
 */
public class AsyncRunLogger implements AutoCloseable {
    // ----- Singleton per run (optional; you can also new() per run) -----
    private static volatile AsyncRunLogger INSTANCE;
    public static AsyncRunLogger get(){ return INSTANCE; }

    // ----- Fields -----
    private final LoggerConfig cfg;
    private final String runId; // folder name
    private final Path runDir;
    private final Path eventsPath;
    private final Path summaryPath;
    private final Path errorsPath;
    private final Path metadataPath;

    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(64_000);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread writerThread;

    private volatile long lastFlushNanos = System.nanoTime();
    private final List<String> batch = new ArrayList<>(2048);

    /**
     * Creates the per-run folder and files, open a thread to run the AsyncLogger
     * @param cfg configuration of logger
     * @param metadata metadata for the run
     * @throws IOException
     * @implNote TODO move the summary header string to the LogModels class
     */
    private AsyncRunLogger(LoggerConfig cfg, Metadata metadata) throws IOException {
        this.cfg = cfg;
        String ts = LocalDateTime.now().toString().replace(':','-');
        this.runId = ts + "_runTag=" + cfg.runTag;
        this.runDir = cfg.baseDir.resolve(runId);
        Files.createDirectories(runDir);

        this.eventsPath   = runDir.resolve("events.jsonl");
        this.summaryPath  = runDir.resolve("summary.csv");
        this.errorsPath   = runDir.resolve("errors.log");
        this.metadataPath = runDir.resolve("metadata.json");

        // summary header (if enabled)
        if (cfg.summaryEnabled) {
            writeString(summaryPath,
                    Summary.CSV_HEADER,
                    CREATE, TRUNCATE_EXISTING, WRITE);
        }

        // metadata
        writeString(metadataPath, metadata.toJson() + "\n", CREATE, TRUNCATE_EXISTING, WRITE);

        // writer thread
        writerThread = new Thread(this::drainLoop, "AsyncRunLogger-Writer");
        writerThread.setDaemon(true);
        writerThread.start();

        // JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { close(); } catch (Exception ignored) {}
        }));
    }

    /**
     * Init entry point: validates config, constructs, sets the singleton.
     * Given the LoggerConfig and the Metadata for the run, creates and returns a new instance of the logger.
     * Use this once at run start: var logger = AsyncRunLogger.start(cfg, meta);
     * @param cfg configuration of logger
     * @param metadata metadata for the run
     * @return AsyncRunLogger
     */
    public static AsyncRunLogger start(LoggerConfig cfg, Metadata metadata) {
        Objects.requireNonNull(cfg.baseDir, "LoggerConfig.baseDir required");
        try {
            var logger = new AsyncRunLogger(cfg, metadata);
            INSTANCE = logger;
            return logger;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String runId(){ return runId; }
    public Path runDir(){ return runDir; }

    // ----- Public hot-path API (tiny work; no blocking) -----

    /**
     * Public hot-path API to log an event, non-blocking.
     * Checks eventDetail and eventSample for volume control.
     * Formats a JSONL line and offers to the logger queue.
     * @param e event to log
     * @implNote uses queue.offer(), if the queue is full the log line is quietly DROPPED.
     *           For strict auditing, we can use put instead but might cause backpressure.
     */
    public void event(Event e) {
        if (cfg.eventDetail == LoggerConfig.EventDetail.NONE) return;
        if (!sampleHit(cfg.eventSample)) return;
        offer("E|" + e.toJsonLine());
    }

    /**
     * Public hot-path API to log a summary, non-blocking.
     * Checks if request summary are enabled for volume control
     * Formats a CSV line and offers to the logger queue.
     * @param s summary of a request to log
     * @implNote uses queue.offer(), if the queue is full the log line is quietly DROPPED.
     *           For strict auditing, we can use put instead but might cause backpressure.
     */
    public void summary(Summary s) {
        if (!cfg.summaryEnabled) return;
        offer("S|" + s.toCsvLine());
    }

    /**
     * Public hot-path API to log an error, non-blocking.
     * Formats a plain text error record (X|…\n) for errors.log.
     * Optionally mirrors to console
     * @param msg message error
     * @param t
     * @implNote uses queue.offer(), if the queue is full the log line is quietly DROPPED.
     *           For strict auditing, we can use put instead but might cause backpressure.
     */
    public void error(String msg, Throwable t) {
        String s = "["+nowIso()+"] ERROR " + msg + (t==null? "" : " :: " + stack(t)) + "\n";
        offer("X|" + s);
        if (cfg.logToConsole) System.err.print(s);
    }

    // ----- Queue & writer loop -----
    // NON BLOCKING
    private void offer(String line) {
        if (closed.get()) return;
        // best-effort offer; drop if queue is full to avoid backpressure in perf runs
        queue.offer(line);
    }

    // BLOCKING
    private void put(String line) {
        if (closed.get()) return;
        try {
            // Blocking: waits if the queue is full until space becomes available
            queue.put(line);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Middle ground, offer with a timeout
    private void offerTimeout(String line) {
        if (closed.get()) return;
        try {
            // Wait up to 10 ms for space in the queue; drop if still full
            boolean ok = queue.offer(line, 10, TimeUnit.MILLISECONDS);
//            if (!ok) {
//                // Optional: count or warn about dropped log lines
//            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();   // preserve interrupt status
        }
    }

    /**
     * This is the loop in the thread dedicated to the Logger. It opens three buffered streams for events/summary/errors,
     * then it loops:
     *  1) polls the queue, accumulates into an in-memory batch;
     *  2) flushes on batch size or time (cfg.flushEvery);
     * until the Logger is asked to close and the queue is empty.
     * It does a final flush on exit.
     */
    private void drainLoop() {
        try (var eventsOut = new BufferedOutputStream(Files.newOutputStream(eventsPath, CREATE, APPEND, WRITE), 1<<20);
             var summaryOut = cfg.summaryEnabled ? new BufferedOutputStream(Files.newOutputStream(summaryPath, CREATE, APPEND, WRITE), 1<<20) : null;
             var errorsOut = new BufferedOutputStream(Files.newOutputStream(errorsPath, CREATE, APPEND, WRITE), 1<<20)) {

            while (!closed.get() || !queue.isEmpty()) {
                String item = queue.poll(5, TimeUnit.MILLISECONDS);
                if (item != null) batch.add(item);

                boolean timeToFlush = (System.nanoTime() - lastFlushNanos) >= cfg.flushEvery.toNanos();
                if (batch.size() >= cfg.batchSize || timeToFlush) {
                    flushBatch(batch, eventsOut, summaryOut, errorsOut);
                    batch.clear();
                    lastFlushNanos = System.nanoTime();
                }
            }

            // final flush
            flushBatch(batch, eventsOut, summaryOut, errorsOut);

        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException ioe) {
            System.err.println("AsyncRunLogger writer crashed: " + ioe);
        }
    }

    /**
     * Flush the given lines into the corresponding Output streams.
     * If cfg.writeParanoid is true, it forces the OS to flush the file’s dirty pages from the page cache to the physical storage.
     * @param lines list of Strings, each with a prefix to indicate in which buffer to write it.
     * @param eventsOut OutputStream of events
     * @param summaryOut OutputStream of summary
     * @param errorsOut OutputStream of errors
     * @throws IOException
     */
    private void flushBatch(List<String> lines,
                            OutputStream eventsOut,
                            OutputStream summaryOut,
                            OutputStream errorsOut) throws IOException {
        if (lines.isEmpty()) return;
        // Demultiplex by prefix to avoid multi-file writers in hot path
        for (String s : lines) {
            if (s.startsWith("E|")) {
                write(eventsOut, s, 2);
            } else if (s.startsWith("S|") && summaryOut != null) {
                write(summaryOut, s, 2);
            } else if (s.startsWith("X|")) {
                write(errorsOut, s, 2);
            }
        }
        if (cfg.writeParanoid) {
            ((FileChannel) FileChannel.open(eventsPath, WRITE)).force(true);
            if (summaryOut != null) ((FileChannel) FileChannel.open(summaryPath, WRITE)).force(true);
            ((FileChannel) FileChannel.open(errorsPath, WRITE)).force(true);
        }
    }

    // Write a string in buffer, consume the prefix used to choose the Output stream
    private void write(OutputStream out, String s, int skipPrefix) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8), skipPrefix, s.length()-skipPrefix);
    }

    // ----- Helpers -----
    private static boolean sampleHit(double p){ return p >= 1.0 || ThreadLocalRandom.current().nextDouble() < p; }
    private static String nowIso(){ return java.time.Instant.now().toString(); }

    private static void writeString(Path p, String s, OpenOption... opts) throws IOException {
        Files.createDirectories(p.getParent());
        try (var out = Files.newOutputStream(p, opts)) {
            out.write(s.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String stack(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try { writerThread.join(2000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}