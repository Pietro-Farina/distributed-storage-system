package it.unitn.ds1.protocol;

import akka.actor.ActorRef;
import it.unitn.ds1.DataItem;
import it.unitn.ds1.utils.OperationUid;

import java.io.Serializable;
import java.util.*;

public class Messages {
    public enum Status { OK, BUSY }

    public static class JoinNetworkMsg implements Serializable {
        public final Map<Integer, ActorRef> network;

        public JoinNetworkMsg(Map<Integer, ActorRef> network) {
            this.network = Collections.unmodifiableMap(new TreeMap<>(network));
        }
    }

    public static class StartUpdateMSg implements Serializable {
        public final int dataKey;
        public final String value;
        public final ActorRef node;

        public StartUpdateMSg(int dataKey, String value, ActorRef node) {
            this.dataKey = dataKey;
            this.value = value;
            this.node = node;
        }
    }

    public static class StartGetMsg implements Serializable {
        public final int dataKey;
        public final ActorRef node;

        public StartGetMsg(int dataKey, ActorRef node) {
            this.dataKey = dataKey;
            this.node = node;
        }
    }

    public static class QueueUpdateMsg implements Serializable {
        public final int dataKey;
        public final String value;
        public final ActorRef node;

        public QueueUpdateMsg(int dataKey, String value, ActorRef node) {
            this.dataKey = dataKey;
            this.value = value;
            this.node = node;
        }
    }

    public static class QueueGetMsg implements Serializable {
        public final int dataKey;
        public final ActorRef node;

        public QueueGetMsg(int dataKey, ActorRef node) {
            this.dataKey = dataKey;
            this.node = node;
        }
    }

    public static class UpdateRequestMsg implements Serializable {
        public final int dataKey;
        public final String value;
        public final OperationUid operationUid;

        // Used by a client to create a message to send a node
        public UpdateRequestMsg(int dataKey, String value) {
            this.dataKey = dataKey;
            this.value = value;
            this.operationUid = null;
        }

        // Used by the coordinator to create a message to send the nodes responsible for the dataItem
        public UpdateRequestMsg(int dataKey, String value, OperationUid operationUid) {
            this.dataKey = dataKey;
            this.value = value;
            this.operationUid = operationUid;
        }
    }

    public static class UpdateResponseMsg implements Serializable {
        public final Status status;
        public final int key;
        public final DataItem value;
        public final OperationUid operationUid;
        public final int senderKey;

        public UpdateResponseMsg(Status status, int key, DataItem value, OperationUid operationUid, int senderKey) {
            this.status = status;
            this.key = key;
            this.value = value;
            this.operationUid = operationUid;
            this.senderKey = senderKey;
        }
    }

    public static class UpdateResultMsg implements Serializable {
        public final int clientOperationNumber;
        public final int dataKey;
        public final DataItem value;
        public final OperationUid operationUid;

        public UpdateResultMsg(int clientOperationNumber,
                               int dataKey, DataItem value, OperationUid operationUid) {
            this.clientOperationNumber = clientOperationNumber;
            this.dataKey = dataKey;
            this.value = value;
            this.operationUid = operationUid;
        }
    }

    public static class GetRequestMsg implements Serializable {
        public final int dataKey;
        public final OperationUid operationUid;

        // Used by the client to create a message to send to a node
        public GetRequestMsg(int dataKey) {
            this.dataKey = dataKey;
            this.operationUid = null;
        }

        // Used by the coordinator to create a message to send the nodes responsible for the dataItem
        public GetRequestMsg(int dataKey, OperationUid operationUid) {
            this.dataKey = dataKey;
            this.operationUid = operationUid;
        }
    }

    /**
     * Actually the operationUid and the DataItem is all I need
     * as I can obtain the dataKey from the operationUid
     * Status is also not necessary, but can be used for sake of clarity
     */
    public  static class GetResponseMsg implements Serializable {
        public final Status status;
        public final OperationUid operationUid;
        public final DataItem value;
        public final int senderKey;

        public GetResponseMsg(Status status, OperationUid operationUid, DataItem value, int senderKey) {
            this.status = status;
            this.operationUid = operationUid;
            this.value = value;
            this.senderKey = senderKey;
        }
    }

    public static class GetResultMsg implements Serializable {
        public final int clientOperationNumber;
        public final int dataKey;
        public final DataItem value;

        public GetResultMsg(int clientOperationNumber,
                            int dataKey, DataItem value) {
            this.clientOperationNumber = clientOperationNumber;
            this.dataKey = dataKey;
            this.value = value;
        }
    }

    public static class StartJoinMsg implements Serializable {
        public final int newNodeKey;
        public final ActorRef bootstrapNode;

        public StartJoinMsg(int newNodeKey, ActorRef bootstrapNode) {
            this.newNodeKey = newNodeKey;
            this.bootstrapNode = bootstrapNode;
        }
    }

    public static class BootstrapMsg implements Serializable {
        public final int newNodeKey;
        public final NavigableMap<Integer, ActorRef> network;

        public BootstrapMsg(int newNodeKey, NavigableMap<Integer, ActorRef> network) {
            this.newNodeKey = newNodeKey;
            this.network = network;
        }
    }

    public static class RequestDataMsg implements Serializable {
        public final int newNodeKey;
        public final Map<Integer, DataItem> requestedData;

        public RequestDataMsg(int newNodeKey, Map<Integer, DataItem> requestedData) {
            this.newNodeKey = newNodeKey;
            this.requestedData = requestedData;
        }
    }

    public static class AnnounceNodeMsg implements Serializable {
        public final int newNodeKey;
        public final ActorRef node;

        public AnnounceNodeMsg(int newNodeKey, ActorRef node) {
            this.newNodeKey = newNodeKey;
            this.node = node;
        }
    }

    public static class Error implements Serializable {
        public final int clientOperationNumber;

        public Error(int clientOperationNumber) {
            this.clientOperationNumber = clientOperationNumber;
        }
    }

    public static class ErrorMsg implements Serializable {
        public final String reason;

        public ErrorMsg(String reason) {
            this.reason = reason;
        }
    }

    public static class Timeout implements Serializable {
        public final OperationUid operationUid;
        public final int dataKey;

        public Timeout(OperationUid operationUid, int dataKey) {
            this.operationUid = operationUid;
            this.dataKey = dataKey;
        }
    }
}
