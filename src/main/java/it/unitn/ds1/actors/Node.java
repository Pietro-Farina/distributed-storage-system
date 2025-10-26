package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.DataItem;
import it.unitn.ds1.protocol.KeyDataOperationRef;
import it.unitn.ds1.protocol.KeyOperationRef;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.protocol.Operation;
import it.unitn.ds1.utils.ApplicationConfig;
import it.unitn.ds1.utils.OperationUid;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
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

    /**
     * TODO: What if we timeout and then it arrives? We should accept the data but we might have given the lock to another resource. TIMEOUT should be > round trip time
     * TODO: What if the timeout time is the start of the timeout
     * @param updateResultMsg
     */
    private void onUpdateResultMsg(Messages.UpdateResultMsg updateResultMsg) {
        // Commit the update
        storage.put(updateResultMsg.dataKey, updateResultMsg.value);

        // Release the replica lock
        releaseReplicaLock(updateResultMsg.dataKey);

        // Cancel the timer - to avoid stale timeout
        Cancellable timer = lockTimers.remove(updateResultMsg.operationUid);
        if (timer != null) { timer.cancel(); } // should always be not null?
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
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, dataKey, replicationParameters.N);

        OperationUid operationUid = nextOperationUid();
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
        operation.timer = scheduleTimeout(replicationParameters.T, operationUid, dataKey);
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
        lockTimers.put(
                updateRequestMsg.operationUid,
                scheduleTimeout(replicationParameters.T, updateRequestMsg.operationUid, dataKey)
        );
    }

    private void finishUpdateSuccess(Operation operation) {
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
    }

    private void finishUpdateFail(Operation operation, String reason) {
        // abort replicas? WE COULD DO IT and it would get faster, but they asked the least possible messages so
        // we will let it timeout TODO: write this reasoning - comparable analysis?

        // if we locked locally, we have to release
        if (getResponsibleNodesKeys(network, operation.dataKey, replicationParameters.N).contains(this.id)) {
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
        Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, dataKey, replicationParameters.N);
        OperationUid operationUid = nextOperationUid();
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
        operation.timer = scheduleTimeout(replicationParameters.T, operationUid, dataKey);

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
            } else if (operation.operationType.equals("GET")) {
                finishGetFail(operation, "TIMEOUT");
            } else if (operation.operationType.equals("JOIN")) {
                finishJoinFail();
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

    /**
     * Only Joining node handles this
     * @param startJoinMsg from NetworkManager
     */
    private void onStartJoinMsg(Messages.StartJoinMsg startJoinMsg) {
        // the node is already in the network
        if (network.containsKey(startJoinMsg.newNodeKey)) {
            // TODO error msg
            return;
        }
        // Wrong ID
        if (this.id != startJoinMsg.newNodeKey) {
            // TODO error msg
            return;
        }

        // create the operation
        OperationUid operationUid = nextOperationUid();
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
    }

    /**
     * Only bootstrap nodes answer this
     * @param bootstrapRequestMsg from Joining Node
     */
    private void onBootstrapRequestMsg(Messages.BootstrapRequestMsg bootstrapRequestMsg) {
        // Sending the node in the network
        Messages.BootstrapResponseMsg responseMsg = new Messages.BootstrapResponseMsg(
                bootstrapRequestMsg.joiningOperationUid,
                this.network
        );
        sender().tell(responseMsg, self());
    }

    /**
     * Only joining nodes answer this
     * It could be a stale message
     * @param bootstrapResponseMsg from Bootstrap Node
     */
    private void onBootstrapResponseMsg(Messages.BootstrapResponseMsg bootstrapResponseMsg) {
        if (!coordinatorOperations.containsKey(bootstrapResponseMsg.joiningOperationUid))
            return; // stale message

        this.network.clear();
        this.network.putAll(bootstrapResponseMsg.network);

        // Get the following node in the ring
        final int successorKey = getSuccessorNodeKey(this.id);
        ActorRef successorNode = network.get(successorKey);

        // Request the node I will have to store to the following node in the ring
        Messages.RequestDataMsg requestMsg = new Messages.RequestDataMsg(
                bootstrapResponseMsg.joiningOperationUid,
                this.id
        );
        successorNode.tell(requestMsg, self());
    }

    /**
     * Any node in the network can receive this request from a Joining Node.
     * @param requestDataMsg from Joining Node
     */
    private void onRequestDataMsg(Messages.RequestDataMsg requestDataMsg) {
        // Send only the data the joining node has to store
        Map<Integer, DataItem> requestedData = computeItemsForJoiner(requestDataMsg.newNodeKey);
        Messages.ResponseDataMsg responseMsg = new Messages.ResponseDataMsg(
                requestDataMsg.joiningOperationUid,
                requestedData,
                this.id
        );
        sender().tell(responseMsg, self());
    }

    /**
     * Only joining node answer this.
     * It could be a stale message.
     * @param responseDataMsg from Successor Node of the Joining Node
     */
    private void onResponseDataMsg(Messages.ResponseDataMsg responseDataMsg) {
        if (!coordinatorOperations.containsKey(responseDataMsg.joiningOperationUid))
            return; // stale message

        // We have the check on N == 1 as in startReadingPhase we ping the other nodes in the network and wait for their answer
        if (replicationParameters.N == 1 || responseDataMsg.requestedData.isEmpty()) {
            storage.putAll(responseDataMsg.requestedData); // save the possible data in the storage
            finishJoinSuccess(responseDataMsg.joiningOperationUid); // trivial case
            return;
        }
        startReadingPhase(responseDataMsg); // R-quorum on each key
    }

    /**
     * Any node in the network can receive this request from a Joining Node.
     * @param readDataRequestMsg from Joining Node
     */
    private void onReadDataRequestMsg(Messages.ReadDataRequestMsg readDataRequestMsg) {
        final List <KeyDataOperationRef> requestedData = new ArrayList<>();
        for (KeyOperationRef ref : readDataRequestMsg.requestedData) {
            final DataItem item = storage.get(ref.key());
            requestedData.add(new KeyDataOperationRef(
                    ref.key(),
                    item,
//                    item != null ? item : new DataItem(null, 0),
                    ref.opId()
            ));
        }
        Messages.ReadDataResponseMsg responseMsg = new Messages.ReadDataResponseMsg(
                readDataRequestMsg.joiningOperationUid,
                requestedData,
                this.id
        );
        sender().tell(responseMsg, self());
    }

    /**
     * Only joining node answer this.
     * It could be a stale message.
     * @param readDataResponseMsg from a Node that contains items which the Joining Node has to store
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
            return;
        }

        if (joiningOperation.quorumTracker.hasQuorum()) {
            finishJoinSuccess(readDataResponseMsg.joiningOperationUid);
        } else {
            finishJoinFail();
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
    }

    /**
     * Start the reading phase of the joining operation.
     * It creates for each item to store a reading r-quorum operation. If any fails the join fails
     * It is little expensive -> O(requestedData.size() * network.size()) however we request and receive only the
     * data we actually need from each node.
     * @param responseDataMsg from the Successor Node of the Joining Node
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

        final int successorNodeKey = getSuccessorNodeKey(this.id);

        // Create a per-item quorum
        for (Map.Entry<Integer, DataItem> item : responseDataMsg.requestedData.entrySet()) {
            // define the current responsible nodes for holding the key, note the joining node is not the network yet
            final Set<Integer> responsibleNodesKeys = getResponsibleNodesKeys(network, item.getKey(), replicationParameters.N);

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
            perKeyReadOp.onOkResponse(successorNodeKey, item.getValue());

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

        // Multicast Here I am
        Messages.AnnounceNodeMsg nodeMsg = new Messages.AnnounceNodeMsg(this.id);
        multicastMessage(network.keySet(), nodeMsg);

        // Add myself to the network
        network.put(this.id, self());

        // Closing all Operations
        coordinatorOperations.remove(joiningOperationUid);
    }

    private void finishJoinFail() {
        // Clear all maps and variables
        coordinatorOperations.clear();
        storage.clear();
        network.clear();
        // TODO write error msg
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

    private int getSuccessorNodeKey(int key) {
        Integer cur = network.ceilingKey(key);
        if (cur == null) cur = network.firstKey();

        return cur;
    }

    /**
     * It is a dumb way to compute this O(network.size * storage), there are faster way
     * by computing the intervals for each data key
     * @param nodeKey of the joining node
     * @return a Map with the items the joining node will have to store
     */
    private Map<Integer, DataItem> computeItemsForJoiner(int nodeKey) {
        // Simulate the new ring
        final NavigableMap<Integer, ActorRef> newRing = new TreeMap<>(network);
        newRing.put(nodeKey, null);

        final Map<Integer, DataItem> map = new HashMap<>();
        for (Map.Entry<Integer, DataItem> entry : storage.entrySet()) {
            if (getResponsibleNodesKeys(newRing, entry.getKey(), newRing.size()).contains(nodeKey)) {
                map.put(entry.getKey(), entry.getValue());
            }
        }

        return map;
    }

    /**
     * It is a dumb way to compute this O(network.size * storage), there are faster way
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
                .build();
    }
}
