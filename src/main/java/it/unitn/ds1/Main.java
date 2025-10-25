package it.unitn.ds1;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Node;
import it.unitn.ds1.logging.AsyncRunLogger;
import it.unitn.ds1.logging.LogModels;
import it.unitn.ds1.logging.LoggerConfig;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;

import java.util.*;

public class Main {
    final static int N_NODES = 4;

    public static void main(String[] args) throws InterruptedException {
        // Loads application.conf (+ application-{env}.conf overrides if present)
        // Note that they can be override by command-line, e.g.:
        // -Dds1.replication.N=5 -Dds1.log.eventDetail=NORMAL
        Config raw = ConfigFactory.load();
        ApplicationConfig cfg = ApplicationConfig.from(raw);

        System.out.printf(
                "N=%d R=%d W=%d T=%dms | keySpace=%d | delay=[%d..%d]ms | runsDir=%s | tag=%s eventDetail=%s summaryEnabled=%s logToConsole=%s%n",
                cfg.replication.N, cfg.replication.R, cfg.replication.W, cfg.replication.T,
                cfg.ring.keySpace, cfg.delays.delayMinMs, cfg.delays.delayMaxMs,
                cfg.paths.runsDir, cfg.log.runTag, cfg.log.eventDetail, cfg.log.summaryEnabled, cfg.log.logToConsole
        );

        // Random Generator
        Random rand = new Random(cfg.random.seed);

        // Initialize logger in (main or SimulationRunner)
        var loggerCfg = new LoggerConfig.Builder()
                .runTag(cfg.log.runTag)
                .baseDir(java.nio.file.Paths.get(cfg.paths.runsDir))
                .eventDetail(cfg.log.eventDetail)
                .summaryEnabled(cfg.log.summaryEnabled)
                .eventSample(cfg.log.eventSample)
                .batchSize(cfg.log.batchSize)
                .flushEvery(cfg.log.flushEvery)
                .logToConsole(cfg.log.logToConsole)
                .writeParanoid(cfg.log.writeParanoid)
                .build();
        var meta = new LogModels.Metadata(
                java.time.Instant.now().toString(),
                "test1",      // not used; kept for completeness
                loggerCfg.runTag,
                cfg.replication.N, cfg.replication.R, cfg.replication.W, (int) cfg.replication.T,                  // your run params
                10, 10, 4242,
                System.getProperty("java.version")
        );
        var logger = AsyncRunLogger.start(loggerCfg, meta);

        // Create the Network Manager
        NetworkManager networkManager = new NetworkManager(cfg);

        // Generate N node keys (spread on the ring)
        int nNodes = 8;
        List<Integer> nodeKeysToAdd = new ArrayList<>();
        for (int i = 0; i < nNodes; i++) {
            nodeKeysToAdd.add((i + 1) * 10); // 10, 20, 30, ...
        }

        // Generate M random data entries
        int nDataItems = 25;
        Map<Integer, String> dataToAdd = new LinkedHashMap<>();
        String[] sampleValues = {"iron", "copper", "water", "oxygen", "nickel", "rock", "dust"};

        for (int i = 0; i < nDataItems; i++) {
            int key = rand.nextInt(100);                  // key in [0,100)
            String value = sampleValues[rand.nextInt(sampleValues.length)];
            dataToAdd.put(key, value + "_" + key);        // e.g., "iron_57"
        }

        networkManager.initializeNetwork(nodeKeysToAdd, dataToAdd, true);
        Thread.sleep(5000);

        networkManager.terminate();
    }
}
