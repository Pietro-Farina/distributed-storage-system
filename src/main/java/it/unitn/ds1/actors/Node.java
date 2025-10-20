package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.DataItem;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.protocol.Operation;
import it.unitn.ds1.utils.OperationUid;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
//    private final ActorRef networkManager;

    // The network ring <nodeKey, ActorRef>
    private final NavigableMap<Integer, ActorRef> network;
    private final int id;

    // DataStore <storageKey, DataItem>
    private final Map<Integer, DataItem> storage;
    private final Set<Integer> storageLocks;

    // To track operations as Coordinator
    private int opCounter;
    private final Map<OperationUid, Operation> coordinatorOperations;

    // Coordinator guard for this.key to avoid overlapping prepares from this coordinator
    private final Set<Integer> coordinatorGuards;

    // To track operations as StorageNode
    private final Map<OperationUid, Cancellable> lockTimers;

    public Node(
//            ActorRef networkManager,
            int id){
//        this.networkManager = networkManager;
        this.network = new TreeMap<>();
        this.id = id;

        this.storage = new TreeMap<>();
        this.storageLocks = new HashSet<>();
        this.coordinatorOperations = new HashMap<>();
        this.coordinatorGuards = new HashSet<>();
        this.opCounter = 0;
        this.lockTimers = new HashMap<>();
    }

    static public Props props(
//            ActorRef networkManager,
            int id){
        return Props.create(Node.class, () -> new Node(
//                networkManager,
                id));
    }

    private void onJoinNetworkMsg(Messages.JoinNetworkMsg joinNetworkMsg) {
        // initialize network
        network.putAll(joinNetworkMsg.network);
    }

    private void onUpdateRequestMsg(Messages.UpdateRequestMsg updateRequestMsg) {
        if (updateRequestMsg.operationUid == null) {
            startUpdateAsCoordinator(updateRequestMsg.dataKey, updateRequestMsg.value);
        } else {
            startUpdateAsReplica(updateRequestMsg);
        }
    }

    private void onUpdateResponseMsg(Messages.UpdateResponseMsg updateResponseMsg) {
        Operation operation = coordinatorOperations.get(updateResponseMsg.operationUid);

        // Stale message
        if (operation == null) return;

        final int senderNodeKey = updateResponseMsg.senderKey;

        if (updateResponseMsg.status == Messages.Status.BUSY) {
            operation.onBusyResponse(senderNodeKey);
        } else {
            operation.onOkResponse(senderNodeKey, updateResponseMsg.value);
        }

        if (!operation.quorumTracker.done()) return;

        if (operation.quorumTracker.hasQuorum()) {
            finishUpdateSuccess(operation);
        } else {
            finishUpdateFail(operation, "NO QUORUM");
        }
    }

    private void onUpdateResultMsg(Messages.UpdateResultMsg updateResultMsg) {
        // Commit the update
        storage.put(updateResultMsg.dataKey, updateResultMsg.value);

        // Release the replica lock
        releaseReplicaLock(updateResultMsg.dataKey);

        // Cancel the timer - to avoid stale timeout
        Cancellable timer = lockTimers.remove(updateResultMsg.operationUid);
        if (timer != null) { timer.cancel(); } // should always be not null
    }

    private void onGetRequestMsg(Messages.GetRequestMsg getRequestMsg) {
        if (getRequestMsg.operationUid == null) {
            startGetAsCoordinator(getRequestMsg.dataKey);
        } else {
            performGetAsReplica(getRequestMsg);
        }
    }

    private void onGetResponseMsg(Messages.GetResponseMsg getResponseMsg) {
        Operation operation = coordinatorOperations.get(getResponseMsg.operationUid);

        // Stale message
        if (operation == null) return;

        final int senderNodeKey = getResponseMsg.senderKey;

        if (getResponseMsg.status == Messages.Status.BUSY) {
            operation.onBusyResponse(senderNodeKey);
        } else {
            operation.onOkResponse(senderNodeKey, getResponseMsg.value);
        }

        if (!operation.quorumTracker.done()) return;

        if (operation.quorumTracker.hasQuorum()) {
            finishGetSuccess(operation);
        } else {
            finishGetFail(operation, "NO QUORUM");
        }
    }

    private void startUpdateAsCoordinator(int dataKey, String value) {
        // guard at coordinator (optional but recommended)
        // TODO write reasons of guard at coordinator
        if (!acquireCoordinatorGuard(dataKey)) {
            getSender().tell(new Messages.ErrorMsg("COORDINATOR BUSY ON THAT KEY"), self());
            return;
        }

        // check the nodes responsible for the request
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(dataKey);

        int W = 2, T = 1000; // TODO get replication params from config
        OperationUid operationUid = nextOperationUid();
        Operation operation = new Operation(
                dataKey,
                responsibleNodesKeys,
                W,
                getSender(),
                "UPDATE",
                value,
                operationUid
        );
        coordinatorOperations.put(operationUid, operation);

        // If this node is also a replica, try to lock locally first
        if (responsibleNodesKeys.contains(this.id)) {
            if (!acquireReplicaLock(dataKey)) {
                // Busy locally -> fast fail
                // TODO write reasons of why reject if busy locally
                finishUpdateFail(operation, "LOCAL BUSY");
                return;
            }
            // If I got the lock -> remove myself from the recipient to avoid sending me a message
            responsibleNodesKeys.remove(this.id);
        }

        // Send the update request to the other nodes
        Messages.UpdateRequestMsg requestMsg = new Messages.UpdateRequestMsg(
            dataKey,
            value,
            operationUid
        );
        multicastMessage(responsibleNodesKeys, requestMsg);

        // Start the timer
        operation.timer = scheduleTimeout(T, operationUid, dataKey);
    }

    // TODO write reason why we include the senderId when texting Coord -> avoid O(n) to getNodeKey()
    private void startUpdateAsReplica(Messages.UpdateRequestMsg updateRequestMsg) {
        final int dataKey = updateRequestMsg.dataKey;
        final ActorRef coordinator = getSender();

        // Try to acquire lock, if not send back BUSY
        if (!acquireReplicaLock(dataKey)) {
            Messages.UpdateResponseMsg responseMsg = new Messages.UpdateResponseMsg(
                    Messages.Status.BUSY,
                    dataKey,
                    null,
                    updateRequestMsg.operationUid,
                    this.id);
            coordinator.tell(responseMsg, self());
            return;
        }

        // send Messages
        DataItem currentDataItem = storage.get(dataKey);
        Messages.UpdateResponseMsg responseMsg = new Messages.UpdateResponseMsg(
                Messages.Status.OK,
                dataKey,
                currentDataItem,
                updateRequestMsg.operationUid,
                this.id);
        coordinator.tell(responseMsg, self());

        // setup Timeout for the request
        int T = 1000; // TODO get replication params from config
        lockTimers.put(
                updateRequestMsg.operationUid,
                scheduleTimeout(T, updateRequestMsg.operationUid, dataKey)
        );
    }

    private void finishUpdateSuccess(Operation operation) {
        final int chosenVersion = operation.chosenVersion == null ? 1 :
                operation.chosenVersion.getVersion() + 1;
        DataItem committedDataItem = new DataItem(operation.proposedValue, chosenVersion);

        // local commit if we are a replica
        Set <Integer> responsibleNodesKeys = getResponsibleNodesKeys(operation.dataKey);
        if (responsibleNodesKeys.contains(this.id)) {
            storage.put(operation.dataKey, committedDataItem);
            releaseReplicaLock(operation.dataKey);
            // remove myself from the list to avoid sending me a message
            responsibleNodesKeys.remove(this.id);
        }

        // reply to client and multicast to other replicas
        Messages.UpdateResultMsg resultMsg = new Messages.UpdateResultMsg(
                -1, operation.dataKey, committedDataItem, operation.operationUid);
        operation.client.tell(resultMsg, self());
        multicastMessage(responsibleNodesKeys, resultMsg);

        // cleanup: free locks and cancel timer
        cleanup(operation);
    }

    private void finishUpdateFail(Operation operation, String reason) {
        // abort replicas? WE COULD DO IT and it would get faster, but they asked the least possible messages so
        // we will let it timeout TODO: write this reasoning - comparable analysis?

        // if we locked locally, we have to release
        if (getResponsibleNodesKeys(operation.dataKey).contains(this.id)) {
            releaseReplicaLock(operation.dataKey);
        }

        // send the error to the client
        operation.client.tell(new Messages.ErrorMsg(reason), self());

        // cleanup: free locks and cancel timer
        cleanup(operation);
    }

    private void startGetAsCoordinator(int dataKey) {
        if (!acquireCoordinatorGuard(dataKey)) {
            getSender().tell(new Messages.ErrorMsg("COORDINATOR BUSY ON THAT KEY"), self());
        }
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(dataKey);
        int R = 2, T = 1000; // TODO get replication params from config
        OperationUid operationUid = nextOperationUid();
        Operation operation = new Operation(
                dataKey,
                responsibleNodesKeys,
                R,
                getSender(),
                "GET",
                null,
                operationUid
        );
        coordinatorOperations.put(operationUid, operation);

        // If this node is also a replica, try to lock locally first
        if (responsibleNodesKeys.contains(this.id)) {
            if (!acquireReplicaLock(dataKey)) {
                // Busy locally -> fast fail
                // TODO write reasons of why reject if busy locally
                finishUpdateFail(operation, "LOCAL BUSY"); // TODO change to a general Fail
                return;
            }
            // read my value
            operation.onOkResponse(this.id, storage.get(dataKey));

            // Free the locks since is only during the read
            releaseReplicaLock(dataKey);

            // Remove myself from the recipient to avoid sending me a message
            responsibleNodesKeys.remove(this.id);

            // If R=1 we could already have finished processing the request
            // We just need to check if we have quorum -> if N = 1 we already checked the case of no possible quorum by BUSY
            if (operation.quorumTracker.hasQuorum()) {
                finishGetSuccess(operation);
                cleanup(operation);
                return;
            }
        }

        // Send the get request to the other nodes
        Messages.GetRequestMsg requestMsg = new Messages.GetRequestMsg(
                dataKey,
                operationUid
        );
        multicastMessage(responsibleNodesKeys, requestMsg);

        // Start the timer
        operation.timer = scheduleTimeout(T, operationUid, dataKey);

    }

    private void performGetAsReplica(Messages.GetRequestMsg getRequestMsg) {
        final int dataKey = getRequestMsg.dataKey;
        final ActorRef coordinator = getSender();

        // Try to acquire lock, if not send back BUSY
        if (!acquireReplicaLock(dataKey)) {
            Messages.GetResponseMsg responseMsg = new Messages.GetResponseMsg(
                    Messages.Status.BUSY,
                    getRequestMsg.operationUid,
                    null,
                    this.id);
            coordinator.tell(responseMsg, self());
            return;
        }

        // send Messages
        DataItem currentDataItem = storage.get(dataKey);
        Messages.GetResponseMsg responseMsg = new Messages.GetResponseMsg(
                Messages.Status.OK,
                getRequestMsg.operationUid,
                currentDataItem,
                this.id);
        coordinator.tell(responseMsg, self());

        // We can free the lock
        releaseReplicaLock(dataKey);
    }

    private void finishGetSuccess(Operation operation) {
        final DataItem chosenVersion = operation.chosenVersion == null ?
                new DataItem(null, 0) :
                operation.chosenVersion;

        // reply to client
        Messages.GetResultMsg resultMsg = new Messages.GetResultMsg(
                -1, operation.dataKey, chosenVersion);
        operation.client.tell(resultMsg, self());

        // cleanup: free locks and cancel timer
        cleanup(operation);
    }

    private void finishGetFail(Operation operation, String reason) {
        // send the error to the client
        operation.client.tell(new Messages.ErrorMsg(reason), self());

        // cleanup: free locks and cancel timer
        cleanup(operation);
    }

    private void cleanup(Operation operation) {
        // Avoid stale timeout
        if (operation.timer != null) {
            operation.timer.cancel();
        }
        // drop guard and remove the operation
        releaseCoordinatorGuard(operation.dataKey);
        coordinatorOperations.remove(operation.operationUid);
    }

    /**
     * Since we are using Cancellable we shouldn't get STALE timeouts
     * TODO operation == null SHOULD never happen BECAUSE WE CANCEL THE TIMEOUT
     * @param timeout containing {operationUid, dataKey}
     */
    private void onTimeout(Messages.Timeout timeout) {
        Operation operation = coordinatorOperations.get(timeout.operationUid);

        if (this.id == timeout.operationUid.coordinatorId()) { // I am the coordinator for this
            if (operation == null) { // This is a stale timeout, the operation is already finished
                // TODO log: it happened
                return; // Should never happen
            }

            if (operation.operationType.equals("UPDATE")) {
                finishUpdateFail(operation, "TIMEOUT");
            } else {
                finishGetFail(operation, "TIMEOUT");
            }

        } else { // I am a node
            Cancellable timer = lockTimers.remove(timeout.operationUid);
            if (timer == null) { // This is a stale timeout, the operation is already finished
                // TODO log: it happened
                return; // Should never happen
            }
            timer.cancel();
            // Free the write lock
            releaseReplicaLock(timeout.dataKey);
        }
    }




    /* ------------ HELPERS FUNCTIONS ------------ */

    private boolean acquireCoordinatorGuard(int dataKey) {
        return coordinatorGuards.add(dataKey);          // true if acquired
    }

    private void releaseCoordinatorGuard(int dataKey) {
        coordinatorGuards.remove(dataKey);
    }

    private boolean acquireReplicaLock(int dataKey) {
        return storageLocks.add(dataKey);
    }
    private void releaseReplicaLock(int dataKey) {
        storageLocks.remove(dataKey);
    }

    private OperationUid nextOperationUid() {
        return new OperationUid(id, ++opCounter);
    }

    private Cancellable scheduleTimeout(int time, OperationUid operationUid, int dataKey) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                new Messages.Timeout(operationUid, dataKey),
                getContext().system().dispatcher(),
                getSelf());
    }

    private void multicastMessage(Set<Integer> recipientNodes, Serializable msg) {
        for (Integer nodeKey: recipientNodes) {
            ActorRef node = network.get(nodeKey);
            node.tell(msg, self());
        }
    }

    private Set<Integer> getResponsibleNodesKeys(int dataKey){
        // TODO move N to the parameters of the function
        int N = 3;
        if (network.isEmpty()) return Set.of();
        int size = network.size();
        N = Math.min(size, N);

        Set<Integer> responsibleNodesKeys = new HashSet<>();

        // Start at the first key >= k, or wrap to the smallest key
        Integer cur = network.ceilingKey(dataKey);
        if (cur == null) cur = network.firstKey();

        // walk clockwise, wrapping when needed
        for (int i = 0; i < N; i++) {
            responsibleNodesKeys.add(cur);
            cur = network.higherKey(cur);
            if (cur == null) cur = network.firstKey();
        }

        return responsibleNodesKeys;
    }

    // Mapping between the received message types and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.JoinNetworkMsg.class, this::onJoinNetworkMsg)
                .match(Messages.UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(Messages.UpdateResponseMsg.class, this::onUpdateResponseMsg)
                .match(Messages.UpdateResultMsg.class, this::onUpdateResultMsg)
                .match(Messages.GetRequestMsg.class, this::onGetRequestMsg)
                .match(Messages.GetResponseMsg.class, this::onGetResponseMsg)
                .match(Messages.Timeout.class, this::onTimeout)
                .build();
    }
}
