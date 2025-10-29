package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.DataItem;
import it.unitn.ds1.logging.AsyncRunLogger;
import it.unitn.ds1.logging.LogModels;
import it.unitn.ds1.protocol.KeyDataOperationRef;
import it.unitn.ds1.protocol.KeyOperationRef;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.protocol.Operation;
import it.unitn.ds1.utils.ApplicationConfig;
import it.unitn.ds1.utils.OperationUid;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node extends AbstractActor {
//    private final ActorRef networkManager;
    // --------- PARAMETERS FOR QUORUM AND DELAYS OF THE NETWORK ---------
    private final ApplicationConfig.Replication replicationParameters;
    private final ApplicationConfig.Delays delaysParameters;

    // The network ring <nodeKey, ActorRef>
    private final NavigableMap<Integer, ActorRef> network;
    private final int id;

    // DataStore <storageKey, DataItem>
    private final Map<Integer, DataItem> storage;
    private final Set<Integer> storageLocks;

    // To track operations as Coordinator
    private int opCounter;
    private final Map<OperationUid, Operation> coordinatorOperations;

    // Coordinator guard for this.dataKey to avoid overlapping prepares from this coordinator
    private final Set<Integer> coordinatorGuards;

    // To track operations as StorageNode
    private final Map<OperationUid, Cancellable> lockTimers;

    //  To store data from leaving node not yet committed
    private final Map<OperationUid, Map<Integer, DataItem>> uncertainUpdates;

    // ------------- for logging purposes ------------- //
    private final AsyncRunLogger logger = AsyncRunLogger.get();
    private final Map<OperationUid, Long> startTimes;
    private long eventStartTime;
    // ------------------------------------------------ //

    public Node(
            int id,
            ApplicationConfig.Replication replicationParameters,
            ApplicationConfig.Delays delaysParameters){
        this.id = id;
        this.replicationParameters = replicationParameters;
        this.delaysParameters = delaysParameters;

        this.network = new TreeMap<>();
        this.storage = new TreeMap<>();
        this.storageLocks = new HashSet<>();
        this.coordinatorOperations = new HashMap<>();
        this.coordinatorGuards = new HashSet<>();
        this.opCounter = 0;
        this.lockTimers = new HashMap<>();
        this.uncertainUpdates = new HashMap<>();

        this.startTimes = new HashMap<>();
    }

    static public Props props(
            int id, ApplicationConfig.Replication replicationParameters, ApplicationConfig.Delays delaysParameters) {
        return Props.create(Node.class, () -> new Node(
                id, replicationParameters, delaysParameters));
    }

    private void onJoinNetworkMsg(Messages.JoinNetworkMsg joinNetworkMsg) {
        // initialize network
        network.putAll(joinNetworkMsg.network);
    }

    /**
     * Any node can receive this. If the message has no operationUid than it means the message
     * was sent from a Client and the node handling becomes the coordinator for that request.
     * @param updateRequestMsg from Client or Coordinator Node
     */
    private void onUpdateRequestMsg(Messages.UpdateRequestMsg updateRequestMsg) {
        Outcome e;
        if (updateRequestMsg.operationUid == null) {
            e = startUpdateAsCoordinator(updateRequestMsg.dataKey, updateRequestMsg.value);
        } else {
            e = startUpdateAsReplica(updateRequestMsg);
        }
        logEvent(e);
    }

    /**
     * Only the coordinator answer this
     * @param updateResponseMsg from Replica Node
     */
    private void onUpdateResponseMsg(Messages.UpdateResponseMsg updateResponseMsg) {
        Operation operation = coordinatorOperations.get(updateResponseMsg.operationUid);

        // Stale message
        if (operation == null) {
            logEvent(new Outcome(updateResponseMsg.operationUid.toString(), "UPDATE", updateResponseMsg.key, -1, "RESPONSE", false));
            return;
        }

        final int senderNodeKey = updateResponseMsg.senderKey;

        if (updateResponseMsg.status == Messages.Status.BUSY) {
            operation.onBusyResponse(senderNodeKey);
        } else {
            operation.onOkResponse(senderNodeKey, updateResponseMsg.value);
        }

        if (!operation.quorumTracker.done()) {
            logEvent(new Outcome(updateResponseMsg.operationUid.toString(), "UPDATE", updateResponseMsg.key, -1, "RESPONSE", true));
            return;
        }

        Outcome e;
        if (operation.quorumTracker.hasQuorum()) {
            e = finishUpdateSuccess(operation);
        } else {
            finishUpdateFail(operation, "NO QUORUM");
            e = new Outcome(updateResponseMsg.operationUid.toString(), "UPDATE", updateResponseMsg.key, -1, "RESPONSE_FINISHED", false);
        }
        logEvent(e);
    }

    /**
     * TODO: What if we timeout and then it arrives? We should accept the data but we might have given the lock to another resource. TIMEOUT should be > round trip time
     * TODO: What if the timeout time is the start of the timeout
     * @param updateResultMsg by Coordinator
     */
    private void onUpdateResultMsg(Messages.UpdateResultMsg updateResultMsg) {
        // Commit the update
        storage.put(updateResultMsg.dataKey, updateResultMsg.value);

        // Release the replica lock
        releaseReplicaLock(updateResultMsg.dataKey);

        // Cancel the timer - to avoid stale timeout
        Cancellable timer = lockTimers.remove(updateResultMsg.operationUid);
        if (timer != null) { timer.cancel(); } // should always be not null?
        logEvent(new Outcome(updateResultMsg.operationUid.toString(), "UPDATE", updateResultMsg.dataKey, updateResultMsg.value.getVersion(), "REPLICA_COMMIT", true));
    }

    /**
     * Any node can receive this. If the message has no operationUid than it means the message
     * was sent from a Client and the node handling becomes the coordinator for that request.
     * @param getRequestMsg by Client or Coordinator Node
     */
    private void onGetRequestMsg(Messages.GetRequestMsg getRequestMsg) {
        Outcome e;
        if (getRequestMsg.operationUid == null) {
            e = startGetAsCoordinator(getRequestMsg.dataKey);
        } else {
            e = performGetAsReplica(getRequestMsg);
        }
        logEvent(e);
    }

    /**
     * Only the coordinator answer this
     * @param getResponseMsg by Replica Node
     */
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

        Outcome e;
        if (operation.quorumTracker.hasQuorum()) {
            e = finishGetSuccess(operation);
        } else {
            finishGetFail(operation, "NO QUORUM");
            e = new Outcome(operation.operationUid.toString(), "GET", operation.dataKey, -1, "RESPONSE", false);
        }
        logEvent(e);
    }

    private Outcome startUpdateAsCoordinator(int dataKey, String value) {
        OperationUid operationUid = nextOperationUid();
        // guard at coordinator (optional but recommended)
        // TODO write reasons of guard at coordinator
        if (!acquireCoordinatorGuard(dataKey)) {
            getSender().tell(new Messages.ErrorMsg("COORDINATOR BUSY ON THAT KEY", operationUid, "UPDATE", dataKey), self());
            return new Outcome(operationUid.toString(), "UPDATE", dataKey, -1, "COORDINATOR_START", false);
        }

        // check the nodes responsible for the request
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, dataKey, replicationParameters.N);

        Operation operation = new Operation(
                dataKey,
                responsibleNodesKeys,
                replicationParameters.W,
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
                return new Outcome(operationUid.toString(), "UPDATE", dataKey, -1, "COORDINATOR_START", false);
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
        operation.timer = scheduleTimeout(replicationParameters.T, operationUid, dataKey);
        return new Outcome(operationUid.toString(), "UPDATE", dataKey, -1, "COORDINATOR_START", true);
    }

    // TODO write reason why we include the senderId when texting Coord -> avoid O(n) to getNodeKey()
    private Outcome startUpdateAsReplica(Messages.UpdateRequestMsg updateRequestMsg) {
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
            return new Outcome(updateRequestMsg.operationUid.toString(), "UPDATE", dataKey, -1, "REPLICA_WRITE_LOCK_ACQUIRE", false);
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
        lockTimers.put(
                updateRequestMsg.operationUid,
                scheduleTimeout(replicationParameters.T, updateRequestMsg.operationUid, dataKey)
        );
        return new Outcome(updateRequestMsg.operationUid.toString(), "UPDATE", dataKey, currentDataItem == null ? 0 : currentDataItem.getVersion(), "REPLICA_WRITE_LOCK_ACQUIRE", true);
    }

    private Outcome finishUpdateSuccess(Operation operation) {
        final int chosenVersion = operation.chosenVersion == null ? 1 :
                operation.chosenVersion.getVersion() + 1;
        DataItem committedDataItem = new DataItem(operation.proposedValue, chosenVersion);

        // local commit if we are a replica
        Set <Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, operation.dataKey, replicationParameters.N);
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
        return new Outcome(operation.operationUid.toString(), "UPDATE", operation.dataKey, committedDataItem.getVersion(), "RESPONSE_FINISHED", true);
    }

    private void finishUpdateFail(Operation operation, String reason) {
        // abort replicas? WE COULD DO IT and it would get faster, but they asked the least possible messages so
        // we will let it timeout TODO: write this reasoning - comparable analysis?

        // if we locked locally, we have to release
        if (getResponsibleNodesKeys(network, operation.dataKey, replicationParameters.N).contains(this.id)) {
            releaseReplicaLock(operation.dataKey);
        }

        // send the error to the client
        operation.client.tell(new Messages.ErrorMsg(reason, operation.operationUid, "UPDATE", operation.dataKey), self());

        // cleanup: free locks and cancel timer
        cleanup(operation);
    }

    private Outcome startGetAsCoordinator(int dataKey) {
        OperationUid operationUid = nextOperationUid();
        if (!acquireCoordinatorGuard(dataKey)) {
            getSender().tell(new Messages.ErrorMsg("COORDINATOR BUSY ON THAT KEY", operationUid, "GET", dataKey), self());
            return new Outcome(operationUid.toString(), "GET", dataKey, -1, "COORDINATOR_START", false);
        }
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, dataKey, replicationParameters.N);
        Operation operation = new Operation(
                dataKey,
                responsibleNodesKeys,
                replicationParameters.R,
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
                finishUpdateFail(operation, "LOCAL BUSY");
                return new Outcome(operationUid.toString(), "GET", dataKey, -1, "COORDINATOR_START", false);
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
                return new Outcome(operationUid.toString(), "GET", dataKey, -1, "COORDINATOR_START_GET_COMPLETE", true);
            }
        }

        // Send the get request to the other nodes
        Messages.GetRequestMsg requestMsg = new Messages.GetRequestMsg(
                dataKey,
                operationUid
        );
        multicastMessage(responsibleNodesKeys, requestMsg);

        // Start the timer
        operation.timer = scheduleTimeout(replicationParameters.T, operationUid, dataKey);
        return new Outcome(operationUid.toString(), "GET", dataKey, -1, "COORDINATOR_START", true);
    }

    private Outcome performGetAsReplica(Messages.GetRequestMsg getRequestMsg) {
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
            return new Outcome(getRequestMsg.operationUid.toString(), "GET", dataKey, -1, "TODO", false);
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
        return new Outcome(getRequestMsg.operationUid.toString(), "GET", dataKey, -1, "TODO", true);
    }

    private Outcome finishGetSuccess(Operation operation) {
        final DataItem chosenVersion = operation.chosenVersion == null ?
                new DataItem(null, 0) :
                operation.chosenVersion;

        // reply to client
        Messages.GetResultMsg resultMsg = new Messages.GetResultMsg(
                operation.operationUid, operation.dataKey, chosenVersion);
        operation.client.tell(resultMsg, self());

        // cleanup: free locks and cancel timer
        cleanup(operation);
        return new Outcome(operation.operationUid.toString(), "GET", operation.dataKey, operation.chosenVersion.getVersion(), "TODO", true);
    }

    private void finishGetFail(Operation operation, String reason) {
        // send the error to the client
        operation.client.tell(new Messages.ErrorMsg(reason, operation.operationUid, "GET", operation.dataKey), self());

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
                logEvent(new Outcome(timeout.operationUid.toString(), "UPDATE", timeout.dataKey, -1, "COORDINATOR_TIMEOUT", false));
            } else if (operation.operationType.equals("GET")) {
                finishGetFail(operation, "TIMEOUT");
                logEvent(new Outcome(timeout.operationUid.toString(), "GET", timeout.dataKey, -1, "COORDINATOR_TIMEOUT", false));
            } else if (operation.operationType.equals("JOIN")) {
                finishJoinFail(operation.operationUid);
                logEvent(new Outcome(timeout.operationUid.toString(), "JOIN", timeout.dataKey, -1, "COORDINATOR_TIMEOUT", false));
            } else if (operation.operationType.equals("LEAVE")) {
                finishLeaveFail(operation);
                logEvent(new Outcome(timeout.operationUid.toString(), "LEAVE", timeout.dataKey, -1, "COORDINATOR_TIMEOUT", false));
            } else if (operation.operationType.equals("RECOVER")) {
                finishRecoverFail(operation.operationUid, "timeout");
                logEvent(new Outcome(timeout.operationUid.toString(), "RECOVER", timeout.dataKey, -1, "COORDINATOR_TIMEOUT", false));
            }
        } else { // I am a node
            Cancellable timer = lockTimers.remove(timeout.operationUid);
            if (timer == null) { // This is a stale timeout, the operation is already finished
                // TODO log: it happened
                return; // Should never happen
            }
            timer.cancel();
            // Free the write lock
            if (timeout.dataKey >= 0)
                releaseReplicaLock(timeout.dataKey);
            logEvent(new Outcome(timeout.operationUid.toString(), "UNKNOWN", timeout.dataKey, -1, "CLIENT_TIMEOUT", false));
        }
    }

    /**
     * Only Joining node handles this
     * @param startJoinMsg from NetworkManager
     */
    private void onStartJoinMsg(Messages.StartJoinMsg startJoinMsg) {
        OperationUid operationUid = nextOperationUid();
        // the node is already in the network
        if (network.containsKey(startJoinMsg.newNodeKey)) {
            printFailOperation("JOIN", startJoinMsg.newNodeKey, "node already exists");
            logEvent(new Outcome(operationUid.toString(), "JOIN", -1, -1, "JOIN_START", false));
            return;
        }
        // Wrong ID
        if (this.id != startJoinMsg.newNodeKey) {
            printFailOperation("JOIN", startJoinMsg.newNodeKey, "node id ["+ this.id +"] differs from given id");
            logEvent(new Outcome(operationUid.toString(), "JOIN", -1, -1, "JOIN_START", false));
            return;
        }
        onStartOperation(operationUid);

        // create the operation
        Operation operation = new Operation(
                -1,
                new HashSet<>(),
                -1,
                getSender(),
                "JOIN",
                null,
                operationUid
        );
        coordinatorOperations.put(operationUid, operation);

        // send the request to the bootstrap node
        Messages.BootstrapRequestMsg bootstrapRequestMsg = new Messages.BootstrapRequestMsg(
                operationUid);
        startJoinMsg.bootstrapNode.tell(bootstrapRequestMsg, self());

        operation.timer = scheduleTimeout(replicationParameters.T, operationUid, -1);
        logEvent(new Outcome(operationUid.toString(), "JOIN", -1, -1, "JOIN_START", true));
    }

    /**
     * Only bootstrap nodes answer this
     * @param bootstrapRequestMsg from Joining/Recovering Node
     */
    private void onBootstrapRequestMsg(Messages.BootstrapRequestMsg bootstrapRequestMsg) {
        // Sending the node in the network
        Messages.BootstrapResponseMsg responseMsg = new Messages.BootstrapResponseMsg(
                bootstrapRequestMsg.joiningOperationUid,
                this.network
        );
        sender().tell(responseMsg, self());
        logEvent(new Outcome(bootstrapRequestMsg.joiningOperationUid.toString(), "JOIN_RECOVER", -1, -1, "BOOTSTRAP", true));
    }

    /**
     * Only joining node and recovery node answer this
     * It could be a stale message
     * @param bootstrapResponseMsg from Bootstrap Node
     */
    private void onBootstrapResponseMsg(Messages.BootstrapResponseMsg bootstrapResponseMsg) {
        Operation operation =  coordinatorOperations.get(bootstrapResponseMsg.joiningOperationUid);
        if (operation == null)
            return; // stale message

        this.network.clear();
        this.network.putAll(bootstrapResponseMsg.network);

        // Get the following node in the ring
        final int successorKey = getSuccessorNodeKey(this.id);
        ActorRef successorNode = network.get(successorKey);

        // Request the node I will have to store to the following node in the ring
        Messages.RequestDataMsg requestMsg = new Messages.RequestDataMsg(
                bootstrapResponseMsg.joiningOperationUid,
                this.id,
                operation.operationType.equals("RECOVER")
        );
        successorNode.tell(requestMsg, self());
        logEvent(new Outcome(bootstrapResponseMsg.joiningOperationUid.toString(), operation.operationType.equals("RECOVER") ? "RECOVER" : "JOIN", -1, -1, "REQUEST_DATA", true));
    }

    /**
     * Any node in the network can receive this request from a Joining Node or a Recovering Node.
     * @param requestDataMsg from Joining/Recovering Node
     */
    private void onRequestDataMsg(Messages.RequestDataMsg requestDataMsg) {
        // Send only the data the joining node has to store
        Map<Integer, DataItem> requestedData;
        if (requestDataMsg.isRecover) {
            // RECOVER: compute with the CURRENT ring (do NOT add the node)
            requestedData = computeItemsForNode(requestDataMsg.newNodeKey, network);
        } else {
            // JOIN: compute with ring + new node
            requestedData = computeItemsForJoiner(requestDataMsg.newNodeKey);
        }
        Messages.ResponseDataMsg responseMsg = new Messages.ResponseDataMsg(
                requestDataMsg.joiningOperationUid,
                requestedData,
                this.id
        );
        sender().tell(responseMsg, self());
        logEvent(new Outcome(requestDataMsg.joiningOperationUid.toString(), requestDataMsg.isRecover ? "RECOVER" : "JOIN", -1, -1, "SEND_DATA", true));
    }

    /**
     * Only joining or recovering node answer this.
     * It could be a stale message.
     * @param responseDataMsg from Successor Node of the Joining/Recovering Node
     */
    private void onResponseDataMsg(Messages.ResponseDataMsg responseDataMsg) {
        Operation operation =  coordinatorOperations.get(responseDataMsg.joiningOperationUid);
        if (operation == null)
            return; // stale message

        // We have the check on N == 1 as in startReadingPhase we ping the other nodes in the network and wait for their answer
        if (replicationParameters.N == 1 || responseDataMsg.requestedData.isEmpty()) {
            storage.putAll(responseDataMsg.requestedData); // save the possible data in the storage
            if (operation.operationType.equals("JOIN"))
                finishJoinSuccess(responseDataMsg.joiningOperationUid); // trivial case
            else
                finishRecoverSuccess(responseDataMsg.joiningOperationUid);
            logEvent(new Outcome(responseDataMsg.joiningOperationUid.toString(), operation.operationType.equals("RECOVER") ? "RECOVER" : "JOIN", -1, -1, "FINISHED", true));
            return;
        }
        startReadingPhase(responseDataMsg); // R-quorum on each key
        logEvent(new Outcome(responseDataMsg.joiningOperationUid.toString(), operation.operationType.equals("RECOVER") ? "RECOVER" : "JOIN", -1, -1, "REQUEST_PER_ITEM_QUORUM", true));
    }

    /**
     * Any node in the network can receive this request from a Joining/Recovering Node.
     * @param readDataRequestMsg from Joining/Recovering Node
     */
    private void onReadDataRequestMsg(Messages.ReadDataRequestMsg readDataRequestMsg) {
        final List <KeyDataOperationRef> requestedData = new ArrayList<>();
        for (KeyOperationRef ref : readDataRequestMsg.requestedData) {
            final DataItem item = storage.get(ref.key());
            requestedData.add(new KeyDataOperationRef(
                    ref.key(),
                    item,
                    ref.opId()
            ));
        }
        Messages.ReadDataResponseMsg responseMsg = new Messages.ReadDataResponseMsg(
                readDataRequestMsg.joiningOperationUid,
                requestedData,
                this.id
        );
        sender().tell(responseMsg, self());
        logEvent(new Outcome(readDataRequestMsg.joiningOperationUid.toString(), "JOIN_RECOVER", -1, -1, "ANSWER_PER_ITEM_QUORUM", true));
    }

    /**
     * Only Joining/Recovering node answer this.
     * It could be a stale message.
     * @param readDataResponseMsg from a Node that contains items which the Joining/Recovering Node has to store
     */
    private void onReadDataResponseMsg(Messages.ReadDataResponseMsg readDataResponseMsg) {
        // Get joining operation
        final Operation joiningOperation = coordinatorOperations.get(readDataResponseMsg.joiningOperationUid);

        if (joiningOperation == null) {
            return; // stale message
        }

        // Get the Data
        for (KeyDataOperationRef ref : readDataResponseMsg.requestedData) {
            Operation perKeyReadOp = coordinatorOperations.get(ref.opUid());
            if (perKeyReadOp == null) {
                continue; // already reached the quorum for that dataKey
            }

            if (ref.item() == null) { // We got an invalid Item -> sign it as a BUSY
                perKeyReadOp.onBusyResponse(readDataResponseMsg.senderKey);
            } else {
                perKeyReadOp.onOkResponse(readDataResponseMsg.senderKey, ref.item());
            }

            if (perKeyReadOp.quorumTracker.done()) {
                if (perKeyReadOp.quorumTracker.hasQuorum()) {
                    if (perKeyReadOp.chosenVersion != null) {
                        DataItem currentItem = storage.get(ref.dataKey());

                        // the version I got might still be smaller than previous
                        if (currentItem == null || currentItem.getVersion() < perKeyReadOp.chosenVersion.getVersion()) {
                            storage.put(ref.dataKey(), perKeyReadOp.chosenVersion);
                        }
                    }
                    joiningOperation.quorumTracker.onOk(ref.dataKey());
                } else {
                    joiningOperation.quorumTracker.onBusy(ref.dataKey());
                }

                // This operation is not needed anymore, it can be closed
                coordinatorOperations.remove(ref.opUid());
            }
        }

        if (!joiningOperation.quorumTracker.done()) {
            logEvent(new Outcome(joiningOperation.operationUid.toString(), joiningOperation.operationType, -1, -1, "RECEIVED_PER_ITEM_QUORUM", true));
            return;
        }

        if (joiningOperation.quorumTracker.hasQuorum()) {
            if (joiningOperation.operationType.equals("JOIN")) {
                finishJoinSuccess(readDataResponseMsg.joiningOperationUid);
                logEvent(new Outcome(joiningOperation.operationUid.toString(), joiningOperation.operationType, -1, -1, "RECEIVED_PER_ITEM_QUORUM_FINISHED", true));
            } else {
                finishRecoverSuccess(readDataResponseMsg.joiningOperationUid);
                logEvent(new Outcome(joiningOperation.operationUid.toString(), joiningOperation.operationType, -1, -1, "RECEIVED_PER_ITEM_QUORUM_FINISHED", true));
            }
        } else {
            if (joiningOperation.operationType.equals("JOIN")) {
                finishJoinFail(readDataResponseMsg.joiningOperationUid);
                logEvent(new Outcome(joiningOperation.operationUid.toString(), joiningOperation.operationType, -1, -1, "RECEIVED_PER_ITEM_QUORUM_FINISHED", false));
            } else {
                finishRecoverFail(readDataResponseMsg.joiningOperationUid, "per-item read quorum not reached"); // mirror of join fail (stop or retry policy)
                logEvent(new Outcome(joiningOperation.operationUid.toString(), joiningOperation.operationType, -1, -1, "RECEIVED_PER_ITEM_QUORUM_FINISHED", false));
            }
        }
    }

    /**
     * All node in the network will receive it as a node complete the joining operation.
     * The receiver message needs to drop the items which are no longer in charge.
     * This is done stupidly the whole network iterate through its data O(storage.size() * network.size())
     * @param announceNodeMsg from JoiningNode
     */
    private void onAnnounceNodeMsg(Messages.AnnounceNodeMsg announceNodeMsg) {
        network.put(announceNodeMsg.newNodeKey, sender());

        List<Integer> itemsToDrop = computeItemsKeysToDrop();
        for (Integer key : itemsToDrop) {
            storage.remove(key);
        }
        logEvent(new Outcome("TODO", "JOIN", -1, -1, "RECEIVED_ANNOUNCE", true));
    }

    /**
     * Start the reading phase of the joining/recovering operation.
     * It creates for each item to store a reading r-quorum operation. If any fails the join fails
     * It is little expensive -> O(requestedData.size() * network.size()) however we request and receive only the
     * data we actually need from each node.
     * @param responseDataMsg from the Successor Node of the Joining/Recovering Node
     */
    private void startReadingPhase(Messages.ResponseDataMsg responseDataMsg) {;
        final Map<Integer, List<KeyOperationRef>> dataToAskPerNodeKey = new HashMap<>();

        // Update the requirement for the joining Operations
        // We need to reach r-quorum for each key
        final Operation joiningOperation = coordinatorOperations.get(responseDataMsg.joiningOperationUid);
        joiningOperation.quorumTracker.setQuorumRequirements(
                responseDataMsg.requestedData.keySet(),
                responseDataMsg.requestedData.size()
        );

        // Create a per-item quorum
        for (Map.Entry<Integer, DataItem> item : responseDataMsg.requestedData.entrySet()) {
            // define the current responsible nodes for holding the key, note the joining node is not the network yet
            // The recovering node instead is already in the network -> we need to remove it before
            NavigableMap<Integer, ActorRef> ring = this.network;
            if (joiningOperation.operationType.equals("RECOVER")) {
                ring = new TreeMap<>(this.network);
                ring.remove(this.id);
            }
            final Set<Integer> responsibleNodesKeys = joiningOperation.operationType.equals("JOIN") ?
                    getResponsibleNodesKeys(network, item.getKey(), replicationParameters.N) :
                    getResponsibleNodesKeys(ring, item.getKey(), replicationParameters.N);

            // Create the dedicated operation
            final OperationUid perKeyReadOpUid = nextOperationUid();
            final Operation perKeyReadOp = new Operation(
                    item.getKey(),
                    responsibleNodesKeys,
                    replicationParameters.R,
                    null,
                    "JOINING-GET",
                    null,
                    perKeyReadOpUid
            );
            coordinatorOperations.put(perKeyReadOpUid, perKeyReadOp);
            // We do not start the timer as there is already one for the joining

            // We already have the data of the successor node
            perKeyReadOp.onOkResponse(responseDataMsg.senderKey, item.getValue());

            // To avoid sending many different message we just send one for each node
            responsibleNodesKeys.remove(responseDataMsg.senderKey);  // don't ask the sender
            for (int nodeKey : responsibleNodesKeys) {
                dataToAskPerNodeKey
                        .computeIfAbsent(nodeKey, _ -> new ArrayList<>())
                        .add(new KeyOperationRef(item.getKey(), perKeyReadOpUid));
            }
        }

        // Send the messages
        for (Map.Entry<Integer, List<KeyOperationRef>> item : dataToAskPerNodeKey.entrySet()) {
            Messages.ReadDataRequestMsg requestMsg = new Messages.ReadDataRequestMsg(
                    responseDataMsg.joiningOperationUid,
                    item.getValue()
            );

            ActorRef node = network.get(item.getKey());
            node.tell(requestMsg, self());
        }
    }

    private void finishJoinSuccess(OperationUid joiningOperationUid) {
        // I want to save into the storage my most recent data -> Already done when reaching per-item quorum
        Operation operation = coordinatorOperations.remove(joiningOperationUid);
        if (operation != null && operation.timer != null)
            operation.timer.cancel();

        // Multicast Here I am
        Messages.AnnounceNodeMsg nodeMsg = new Messages.AnnounceNodeMsg(this.id);
        multicastMessage(network.keySet(), nodeMsg);

        // Add myself to the network
        network.put(this.id, self());
        onEndOperation(joiningOperationUid, "JOIN", -1, true, -1);
    }

    private void finishJoinFail(OperationUid joiningOperationUid) {
        Operation operation = coordinatorOperations.remove(joiningOperationUid);
        if (operation != null && operation.timer != null)
            operation.timer.cancel();

        // Clear all maps and variables
        coordinatorOperations.clear();
        storage.clear();
        network.clear();
        printFailOperation("JOIN", this.id, "per-item read quorum not reached");

        onEndOperation(joiningOperationUid, "JOIN", -1, false, -1);
        // delete the actor
        context().stop(self());
    }

    /**
     * Any active node in the network can receive this.
     * @param startLeaveMsg by NetworkManager
     */
    private void onStartLeaveMsg(Messages.StartLeaveMsg  startLeaveMsg) {
        OperationUid operationUid = nextOperationUid();
        if (!network.containsKey(this.id)) {
            // NetworkManager send to wrong data -> not sure if it can happen since now we delete actors
            printFailOperation("LEAVE", this.id, "actor not in the network yet");
            logEvent(new Outcome(operationUid.toString(), "LEAVE", -1, -1, "START", false));
            return;
        }

        if (network.size() <= replicationParameters.N) {
            printFailOperation("LEAVE", this.id, "network already too small, node leaving would break replication constraint");
            logEvent(new Outcome(operationUid.toString(), "LEAVE", -1, -1, "START", false));
            return;
        }

        onStartOperation(operationUid);
        startHandingDataPhase(operationUid);
        logEvent(new Outcome(operationUid.toString(), "LEAVE", -1, -1, "START", true));
    }

    /**
     * Any node in the network can receive this.
     * @param leaveWarningMsg by Leaving Node
     */
    private void onLeaveWarningMsg(Messages.LeaveWarningMsg leaveWarningMsg) {
        // Save data in uncertain Updates
        uncertainUpdates.put(leaveWarningMsg.leavingOperationUid, leaveWarningMsg.dataToSave);

        // Send Acknowledgment
        Messages.LeaveAckMsg ack = new Messages.LeaveAckMsg(leaveWarningMsg.leavingOperationUid, this.id);
        sender().tell(ack, self());

        // Should I have a timer?
        logEvent(new Outcome(leaveWarningMsg.leavingOperationUid.toString(), "LEAVE", -1, -1, "RECEIVED_WARNING", true));
    }

    /**
     * Only the leaving node handles this.
     * It could be a stale message.
     * @param leaveAckMsg by nodes in the network that will store some data of the Leaving Node
     */
    private void onLeaveAckMsg(Messages.LeaveAckMsg leaveAckMsg) {
        // Get leaving operation
        final Operation leavingOperation = coordinatorOperations.get(leaveAckMsg.leavingOperationUid);

        if (leavingOperation == null) {
            return; // stale message
        }

        leavingOperation.quorumTracker.onOk(leaveAckMsg.senderKey);

        // we don't have a busy case so we can only wait and timeout
        if (leavingOperation.quorumTracker.hasQuorum()) {
            finishLeaveSuccess(leavingOperation);
            logEvent(new Outcome(leaveAckMsg.leavingOperationUid.toString(), "LEAVE", -1, -1, "RECEIVED_ACK_COMPLETE", true));
            return;
        }
        logEvent(new Outcome(leaveAckMsg.leavingOperationUid.toString(), "LEAVE", -1, -1, "RECEIVED_ACK", true));
    }

    /**
     * All active nodes in the network will handle this
     * @param leaveCommitMsg by Leaving Node
     */
    private void onLeaveCommitMsg(Messages.LeaveCommitMsg leaveCommitMsg) {
        // Check if there is still the operation? -> if not IT IS A BIG PROBLEM

        // Remove the Leaving Node from the network
        network.remove(leaveCommitMsg.leavingNodeKey);

        // Commit uncertainUpdates
        Map<Integer, DataItem> dataToSave = uncertainUpdates.get(leaveCommitMsg.leavingOperationUid);
        if (dataToSave == null) {
            // This is a strange case and should never happen
            return;
        }

        // Prune non-responsible items on receive
        List<Integer> itemsToDrop = computeItemsKeysToDrop();
        for (Integer key : itemsToDrop) {
            storage.remove(key);
        };

        // Save only the most recent version
        for (Map.Entry<Integer, DataItem> entry : dataToSave.entrySet()) {
            DataItem current = storage.get(entry.getKey());
            if (current == null || current.getVersion() < entry.getValue().getVersion()) {
                storage.put(entry.getKey(), entry.getValue());
            }
        }

        // Remove the uncertain data
        uncertainUpdates.remove(leaveCommitMsg.leavingOperationUid);
        logEvent(new Outcome(leaveCommitMsg.leavingOperationUid.toString(), "LEAVE", -1, -1, "RECEIVED_COMMIT", true));
    }

    /**
     * Upon the request to leave the network I need to send my data to the right nodes.
     * Similar as we do for the reading we will send the list of what they have to update, however
     * in this case we need to receive confirmation from all the nodes, otherwise we have to fail.
     * This because if even one node is not answering it would mean that we might break the replication requirement
     * (which happens if the busy node does not yet have that item) OR we might have older version of data
     */
    private void startHandingDataPhase(OperationUid operationUid) {
        // create Leaving Operation
        Operation leavingOperation = new Operation(
                -1,
                new HashSet<>(),
                -1,
                getSender(),
                "LEAVE",
                null,
                operationUid
        );
        coordinatorOperations.put(operationUid, leavingOperation);

        final Map<Integer, Map<Integer, DataItem>> dataToAskPerNodeKey = new HashMap<>();
        final NavigableMap<Integer, ActorRef> newRing = new TreeMap<>(network);
        newRing.remove(this.id);

        for (Map.Entry<Integer, DataItem> entry : storage.entrySet()) {
            final Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(newRing, entry.getKey(), replicationParameters.N);

            for (int nodeKey : responsibleNodesKeys) {
                dataToAskPerNodeKey
                        .computeIfAbsent(nodeKey, _ -> new HashMap<>())
                        .put(entry.getKey(), entry.getValue());
            }
        }

        leavingOperation.quorumTracker.setQuorumRequirements(
                dataToAskPerNodeKey.keySet(),
                dataToAskPerNodeKey.size()
        );

        for (Map.Entry<Integer, Map<Integer, DataItem>> entry : dataToAskPerNodeKey.entrySet()) {
            Messages.LeaveWarningMsg leaveWarningMsg = new Messages.LeaveWarningMsg(
                    operationUid,
                    entry.getValue()
            );

            ActorRef node = network.get(entry.getKey());
            node.tell(leaveWarningMsg, self());
        }

        // Start the timer
        leavingOperation.timer = scheduleTimeout(replicationParameters.T, operationUid, -1);
        logEvent(new Outcome(operationUid.toString(), "LEAVE", -1, -1, "START_HANDING_DATA", true));
    }

    private void finishLeaveSuccess(Operation leavingOperation) {
        // Cancel the timer and remove the operation
        if (leavingOperation.timer != null) {
            leavingOperation.timer.cancel();
        }
        coordinatorOperations.remove(leavingOperation.operationUid);

        // Remove myself from the network
        network.remove(this.id);

        Messages.LeaveCommitMsg leaveCommitMsg = new Messages.LeaveCommitMsg(
                this.id,
                leavingOperation.operationUid
        );
        multicastMessage(network.keySet(), leaveCommitMsg);

        // clear all current information
        network.clear();
        storage.clear();
        coordinatorOperations.clear();

        onEndOperation(leavingOperation.operationUid, "LEAVE", -1, true, -1);
        // delete the actor
        context().stop(self());
    }

    private void finishLeaveFail(Operation leavingOperation) {
        printFailOperation("LEAVE", this.id, "TIMEOUT");
        onEndOperation(leavingOperation.operationUid, "LEAVE", -1, false, -1);
        coordinatorOperations.remove(leavingOperation.operationUid);
    }

    private void onStartRecoveryMsg(Messages.StartRecoveryMsg startRecoveryMsg) {
        OperationUid operationUid = nextOperationUid();
        onStartOperation(operationUid);
        Operation recoveryOperation = new Operation(
                -1,
                new HashSet<>(),
                -1,
                getSender(),
                "RECOVER",
                null,
                operationUid
        );
        coordinatorOperations.put(operationUid, recoveryOperation);

        // send Bootstrap Request
        Messages.BootstrapRequestMsg bootstrapRequestMsg = new Messages.BootstrapRequestMsg(
                operationUid
        );
        startRecoveryMsg.bootstrapNode.tell(bootstrapRequestMsg, self());

        // start timer
        recoveryOperation.timer = scheduleTimeout(replicationParameters.T, operationUid, -1);
    }

    private void finishRecoverSuccess(OperationUid operationUid) {
        Operation operation = coordinatorOperations.remove(operationUid);
        if (operation != null && operation.timer != null)
            operation.timer.cancel();

        // Drop keys I no longer own under the CURRENT ring
        for (Integer k : computeItemsKeysToDrop())
            storage.remove(k);

        // Get available to other request
        getContext().become(createReceive());
        onEndOperation(operationUid, "RECOVER", -1, true, -1);
    }

    private void finishRecoverFail(OperationUid operationUid, String reason) {
        Operation operation = coordinatorOperations.remove(operationUid);
        if (operation != null && operation.timer != null)
            operation.timer.cancel();

        // Clear all maps and variables
        coordinatorOperations.clear();
        storage.clear();
        network.clear();
        printFailOperation("RECOVER", this.id, reason);
        onEndOperation(operationUid, "RECOVER", -1, false, -1);
    }

    private void onCrashMsg(Messages.CrashMsg crashMsg) {
        OperationUid operationUid = nextOperationUid();
        // this node become unavailable
        getContext().become(crashed());
        onEndOperation(operationUid,  "CRASH",  -1, true, -1);
        logEvent(new Outcome(operationUid.toString(), "CRASH", -1, -1, "CRASHED", true));
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

    private Set<Integer> getResponsibleNodesKeys(NavigableMap<Integer, ActorRef> network, int dataKey, int subsetSize){
        int N;
        if (network.isEmpty()) return Set.of();
        int size = network.size();
        N = Math.min(size, subsetSize);

        Set<Integer> responsibleNodesKeys = new HashSet<>();

        // Start at the first dataKey >= k, or wrap to the smallest dataKey
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

    private int getSuccessorNodeKey(int nodeKey) {
        Integer cur = network.higherKey(nodeKey);
        if (cur == null) cur = network.firstKey();

        return cur;
    }

    private Map<Integer, DataItem> computeItemsForNode(int nodeKey, NavigableMap<Integer, ActorRef> ring) {
        final Map<Integer, DataItem> map = new HashMap<>();
        for (var e : storage.entrySet()) {
            if (getResponsibleNodesKeys(ring, e.getKey(), replicationParameters.N).contains(nodeKey)) {
                map.put(e.getKey(), e.getValue());
            }
        }
        return map;
    }

    /**
     * It is a dumb way to compute this O(network.size * storage), there are faster ways
     * by computing the intervals for each data key
     * @param nodeKey of the joining node
     * @return a Map with the items the joining node will have to store
     */
    private Map<Integer, DataItem> computeItemsForJoiner(int nodeKey) {
        // Simulate the new ring
        final NavigableMap<Integer, ActorRef> newRing = new TreeMap<>(network);
        newRing.put(nodeKey, null);
        return computeItemsForNode(nodeKey, newRing);
    }

    /**
     * It is a dumb way to compute this O(network.size * storage), there are faster ways
     * by computing the intervals for eachdata key
     * @return a List with the keys that the node is not responsible anymore
     */
    private List<Integer> computeItemsKeysToDrop() {
        final List<Integer> itemsToDrop = new ArrayList<>();
        for (Map.Entry<Integer, DataItem> entry : storage.entrySet()) {
            if (!getResponsibleNodesKeys(network, entry.getKey(), replicationParameters.N).contains(this.id)) {
                itemsToDrop.add(entry.getKey());
            }
        }
        return itemsToDrop;
    }

    private void printFailOperation(String operationType, int nodeKey, String reason) {
        System.out.printf(
                "[Node %s] %s Operation for nodeKey=%d  failed: %s%n",
                getSelf().path().name(), operationType, nodeKey, reason
        );
    }

    // -------------- HELPER FUNCTION FOR LOGGING -------------- //
    private void onStartOperation(OperationUid operationUid) {
        startTimes.put(operationUid, System.nanoTime());
    }
    private void onEndOperation(OperationUid operationUid, String operationType, int dataKey, boolean success, int chosenVersion) {
        long startTime = startTimes.getOrDefault(operationUid, System.nanoTime());
        long endTime = System.nanoTime();
        var s = new LogModels.Summary(
                Instant.ofEpochMilli(startTime/1_000_000).toString(),
                Instant.ofEpochMilli(endTime/1_000_000).toString(),
                operationUid.toString(),
                self().path().name(),
                operationType,
                dataKey,
                chosenVersion,
                success,
                (endTime - startTime) / 1_000_000,
                "",
                replicationParameters.N, replicationParameters.R, replicationParameters.W, replicationParameters.T
        );
        logger.summary(s);
        // ------ NOTIFY THE MANAGER ----------------------------------------------------------- //
        if (success) {
            if (operationType.equals("JOIN")) {
                getContext().actorSelection("/user/networkManagerInbox").tell(
                        new Messages.ManagerNotifyJoin(this.id), self());
            }
            else if (operationType.equals("LEAVE")) {
                getContext().actorSelection("/user/networkManagerInbox").tell(
                        new Messages.ManagerNotifyLeave(this.id), self());
            }
        }
        // ------------------------------------------------------------------------------------- //
    }
    private class Outcome {
        public final String operationUid;
        public final String operationType;
        public final int dataKey;
        public final int chosenVersion;
        public final String phase;
        public final boolean success;

        private Outcome(String operationUid, String operationType, int dataKey, int chosenVersion, String phase, boolean success) {
            this.operationUid = operationUid;
            this.operationType = operationType;
            this.dataKey = dataKey;
            this.chosenVersion = chosenVersion;
            this.phase = phase;
            this.success = success;
        }
    }

    // TODO move runID to the configuration parameter
    private void logEvent(Outcome outcome) {
        long endTime = System.nanoTime();
        var e = new LogModels.Event(
                "TODO",
                this.id,
                outcome.operationUid,
                outcome.operationType,
                outcome.dataKey,
                outcome.chosenVersion,
                outcome.phase,
                endTime - eventStartTime,
                outcome.success
        );
        logger.event(e);
    }
    // --------------------------------------------------------- //
    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        // here: before message handled
        this.eventStartTime = System.nanoTime();
        super.aroundReceive(receive, msg);  // this actually calls your onXxx handlers
        // here: after message handled
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
                .match(Messages.StartJoinMsg.class, this::onStartJoinMsg)
                .match(Messages.BootstrapRequestMsg.class, this::onBootstrapRequestMsg)
                .match(Messages.BootstrapResponseMsg.class, this::onBootstrapResponseMsg)
                .match(Messages.RequestDataMsg.class, this::onRequestDataMsg)
                .match(Messages.ResponseDataMsg.class, this::onResponseDataMsg)
                .match(Messages.ReadDataRequestMsg.class, this::onReadDataRequestMsg)
                .match(Messages.ReadDataResponseMsg.class, this::onReadDataResponseMsg)
                .match(Messages.AnnounceNodeMsg.class, this::onAnnounceNodeMsg)
                .match(Messages.StartLeaveMsg.class, this::onStartLeaveMsg)
                .match(Messages.LeaveWarningMsg.class, this::onLeaveWarningMsg)
                .match(Messages.LeaveAckMsg.class, this::onLeaveAckMsg)
                .match(Messages.LeaveCommitMsg.class, this::onLeaveCommitMsg)
                .match(Messages.CrashMsg.class, this::onCrashMsg)
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .match(Messages.StartRecoveryMsg.class, this::onStartRecoveryMsg)
                .match(Messages.BootstrapResponseMsg.class, this::onBootstrapResponseMsg)
                .match(Messages.ResponseDataMsg.class, this::onResponseDataMsg)
                .match(Messages.ReadDataResponseMsg.class, this::onReadDataResponseMsg)
                .build();
    }
}
