package it.unitn.ds1.logging;

import java.nio.file.Path;
import java.time.Duration;

public class LoggerConfig {
    public enum EventDetail { NONE, MINIMAL, NORMAL, VERBOSE }

    public final String runTag;         // short label for the run
    public final Path baseDir;          // Root directory where per-run folders are created
    public final EventDetail eventDetail;   // controls events.jsonl volume
    public final boolean summaryEnabled;    // summary.csv on/off
    public final double eventSample;        // 0.0..1.0 density to keep
    public final int batchSize;         // e.g., 1024 lines
    public final Duration flushEvery;   // e.g., 20ms, ensures logs appear even if batchSize isn’t reached.
                                        // Smaller -> more I/O; larger -> logs visible later.
    public final boolean logToConsole;  // to debug errors
    public final boolean writeParanoid; // fsync each batch (hurts performance but avoid loosing logs with java crashes)

    private LoggerConfig(Builder b) {
        this.runTag = b.runTag;
        this.baseDir = b.baseDir;
        this.eventDetail = b.eventDetail;
        this.summaryEnabled = b.summaryEnabled;
        this.eventSample = b.eventSample;
        this.batchSize = b.batchSize;
        this.flushEvery = b.flushEvery;
        this.logToConsole = b.logToConsole;
        this.writeParanoid = b.writeParanoid;
    }

    public static class Builder {
        private String runTag = "run";
        private Path baseDir;
        private EventDetail eventDetail = EventDetail.NORMAL;
        private boolean summaryEnabled = true;
        private double eventSample = 1.0;
        private int batchSize = 1024;
        private Duration flushEvery = Duration.ofMillis(20);
        private boolean logToConsole = false;
        private boolean writeParanoid = false;

        public Builder runTag(String v){ this.runTag=v; return this; }
        public Builder baseDir(Path v){ this.baseDir=v; return this; }
        public Builder eventDetail(EventDetail v){ this.eventDetail=v; return this; }
        public Builder summaryEnabled(boolean v){ this.summaryEnabled=v; return this; }
        public Builder eventSample(double v){ this.eventSample=v; return this; }
        public Builder batchSize(int v){ this.batchSize=v; return this; }
        public Builder flushEvery(Duration v){ this.flushEvery=v; return this; }
        public Builder logToConsole(boolean v){ this.logToConsole=v; return this; }
        public Builder writeParanoid(boolean v){ this.writeParanoid=v; return this; }
        public LoggerConfig build(){ return new LoggerConfig(this); }
    }
}