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

import java.util.TreeMap;
import java.util.Map;

public class Main {
    final static int N_NODES = 4;

    public static void main(String[] args) {
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


        final ActorSystem system = ActorSystem.create("distributed-storage-system");

        // Create nodes and put them to a list
        Map<Integer, ActorRef> network = new TreeMap<>();
        for (int i = 0; i < N_NODES; i++) {
            int id = i * 10;
            network.put(id, system.actorOf(Node.props(id), "node" + id));
        }

        // Send join messages to the nodes to inform them of the whole network
        Messages.JoinNetworkMsg start = new Messages.JoinNetworkMsg(network);
        for (Map.Entry<Integer, ActorRef> peer : network.entrySet()) {
            peer.getValue().tell(start, ActorRef.noSender());
        }

        ActorRef client2 = system.actorOf(Client.props(), "client2");
        ActorRef client1 = system.actorOf(Client.props(), "client1");
        ActorRef client3 = system.actorOf(Client.props(), "client3");

        ActorRef client4 = system.actorOf(Client.props(), "client4");
        client1.tell(new Messages.StartUpdateMSg(12, "CCCCCCCCCCCC", network.get(10)), ActorRef.noSender());
        client2.tell(new Messages.StartUpdateMSg(25, "BBBBBBBBBBBB", network.get(20)), ActorRef.noSender());
        client2.tell(new Messages.StartUpdateMSg(12, "AAAAAAAAAA", network.get(10)), ActorRef.noSender());
        client3.tell(new Messages.StartGetMsg(12, network.get(20)), ActorRef.noSender());
        client4.tell(new Messages.StartGetMsg(1, network.get(10)), ActorRef.noSender());

        client1.tell(new Messages.QueueUpdateMsg(12, "CCCCCCCCCCCC", network.get(10)), ActorRef.noSender());
        client1.tell(new Messages.QueueUpdateMsg(25, "BBBBBBBBBBBB", network.get(20)), ActorRef.noSender());
        client1.tell(new Messages.QueueUpdateMsg(30, "AAAAAAAAAA", network.get(10)), ActorRef.noSender());
        // system shutdown
        system.terminate();
    }
}
