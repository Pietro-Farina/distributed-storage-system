package it.unitn.ds1.logging;

import java.time.Instant;

public class LogModels {

    public static final class Event {
        // TODO define better what to track: operation, version, phase, latency
        public final String ts;         // ISO timestamp when the event was logged
        public final String runId;      // ID of the experiment run (matches folder)
        public final String reqId;      // Unique request or operation identifier (not gonna used by nodes)
        public final String nodeId;     // The actor or node generating the event
        public final String operation;  // Operation type TODO: define the type (e.g., WRITE, READ, REPLICA_ACK)
        public final Integer key;        // The key affected by the operation
        public final Integer version;    // Version number being written/read
        public final String phase;      // Substage of operation (COORD_START, QUORUM_OK, etc.) TODO definte the substage
        public final Long latencyMs;  // Duration of that phase or event TODO node start its timestamps and compute latency when before logging
        public final Boolean success;    // Whether the event completed successfully

        public Event(String runId, String reqId, String nodeId,
                     String operation, Integer key, Integer version,
                     String phase, Long latencyMs, Boolean success) {
            this.ts = Instant.now().toString();
            this.runId = runId;
            this.reqId = reqId;
            this.nodeId = nodeId;
            this.operation = operation;
            this.key = key;
            this.version = version;
            this.latencyMs = latencyMs;
            this.phase = phase;
            this.success = success;
        }

        public String toJsonLine() {
            StringBuilder b = new StringBuilder(256);
            b.append('{')
                    .append("\"ts\":").append(quote(ts)).append(',')
                    .append("\"runId\":").append(quote(runId)).append(',')
                    .append("\"reqId\":").append(quote(reqId)).append(',')
                    .append("\"coordinatorId\":").append(quote(nodeId)).append(',')
                    .append("\"operation\":").append(quote(operation)).append(',')
                    .append("\"key\":").append(key==null?"null":key).append(',')
                    .append("\"version\":").append(version==null?"null":version).append(',')
                    .append("\"phase\":").append(quote(phase)).append(',')
                    .append("\"latencyMs\":").append(latencyMs==null?"null":latencyMs).append(',')
                    .append("\"success\":").append(success==null?"null":success)
                    .append("}\n");
            return b.toString();
        }

        private static String quote(String s) {
            return s == null ? "null" : "\"" + s.replace("\"","'") + "\"";
        }

    }

    public static final class Summary {
        // TODO define better what to track: operation, version, phase, latency
        public static final String CSV_HEADER =
                "ts_start,ts_end,req_id,node_id,op,key,chosen_version,success,latency_ms,owners,N,R,W,T\n";

        public final String tsStart;    // Start timestamps of the request
        public final String tsEnd;      // End timestamps of the request
        public final String reqId;     // Operation ID
        public final String coordinator;    // Node that acted as coordinator
        public final String operation;      // Operation type (READ or WRITE) TODO: define type
        public final Integer key;            // Targeted data key
        public final Integer chosenVersion;  // Final version written/read
        public final Boolean success;       // True if quorum succeeded
        public final Long latencyMs;        // End-to-end latency for the operation
        public final String owners;         // List of replica nodes containing the key
        public final int N; public final int R; public final int W; public final int T;

        public Summary(String tsStart, String tsEnd, String reqId,
                       String coordinator, String operation, Integer key,
                       Integer chosenVersion, Boolean success, Long latencyMs,
                       String owners, int N, int R, int W, int T) {
            this.tsStart = tsStart;
            this.tsEnd = tsEnd;
            this.reqId = reqId;
            this.coordinator = coordinator;
            this.operation = operation;
            this.key = key;
            this.chosenVersion = chosenVersion;
            this.success = success;
            this.latencyMs = latencyMs;
            this.owners = owners;
            this.N = N; this.R = R; this.W = W; this.T = T;
        }

        public String toCsvLine() {
            StringBuilder b = new StringBuilder(256);
            b
                    .append(tsStart).append(',')
                    .append(tsEnd).append(',')
                    .append(csv(reqId)).append(',')
                    .append(csv(operation)).append(',')
                    .append(key).append(',')
                    .append(chosenVersion).append(',')
                    .append(success).append(',')
                    .append(latencyMs).append(',')
                    .append(csv(owners)).append(',')
                    .append(N).append(',').append(R)
                    .append(',').append(W).append(',').append(T)
                    .append('\n');
            return b.toString();
        }

        private static String csv(String s) {
            if (s==null) return "";
            return "\"" + s.replace("\"","'") + "\"";
        }
    }

    // One-shot metadata written at run start
    public static final class Metadata {
        public final String startedAtIso;   // Timestamp of when the run began
        public final String runId;          // Folder identifier (timestamped)
        public final String runTag;         // Human label
        // Replication and Quorum
        public final int N; public final int R; public final int W; public final int T;
        public final long delayMinMs; public final long delayMaxMs; // Simulated network delay bounds
        public final long seed;             // Random seed used
        // public final String ringStart;   // Track node membership / ring layout
        // public final String ringEnd;     // Track node membership / ring layout
        public final String jvm;            // Java runtime version

        public Metadata(String startedAtIso, String runId, String runTag,
                        int N, int R, int W, int T,
                        long delayMinMs, long delayMaxMs, long seed,
                        String jvm
                        ) {
            this.startedAtIso = startedAtIso;
            this.runId = runId;
            this.runTag = runTag;
            this.N = N; this.R = R; this.W = W; this.T = T;
            this.delayMinMs = delayMinMs; this.delayMaxMs = delayMaxMs; this.seed = seed;
            this.jvm = jvm;
        }

        public String toJson() {
            String indent = "  "; // two spaces
            StringBuilder b = new StringBuilder(256);
            b.append("{\n")
                    .append(indent).append("\"startedAtIso\": ").append(quote(startedAtIso)).append(",\n")
                    .append(indent).append("\"runId\": ").append(quote(runId)).append(",\n")
                    .append(indent).append("\"runTag\": ").append(quote(runTag)).append(",\n")
                    .append(indent).append("\"N\": ").append(N).append(",\n")
                    .append(indent).append("\"R\": ").append(R).append(",\n")
                    .append(indent).append("\"W\": ").append(W).append(",\n")
                    .append(indent).append("\"T\": ").append(T).append(",\n")
                    .append(indent).append("\"delayMinMs\": ").append(delayMinMs).append(",\n")
                    .append(indent).append("\"delayMaxMs\": ").append(delayMaxMs).append(",\n")
                    .append(indent).append("\"seed\": ").append(seed).append(",\n")
//   .append(indent).append("\"ringSummary\": ").append(quote(ringSummary)).append(",\n")
//   .append(indent).append("\"gitSha\": ").append(quote(gitSha)).append(",\n")
                    .append(indent).append("\"jvm\": ").append(quote(jvm)).append("\n")
                    .append("}");
            return b.toString();
        }

        private static String quote(String s) {
            return s == null ? "null" : "\"" + s.replace("\"","'") + "\"";
        }
    }
}
