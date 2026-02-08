package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.logging.AsyncRunLogger;
import it.unitn.ds1.logging.LogModels;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;
import it.unitn.ds1.utils.OperationUid;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/*
    The clients execute commands passed through messages, print the reply from the
    coordinator, and only then accept further commands. A client may perform multiple
    read and write operations
 */
public class Client extends AbstractActor {
    private ApplicationConfig.Delays delaysParameters;
    private int opCounter;
    private boolean busy;

    // To avoid locking when contacting a crashed node
    private Cancellable operationTimer;

    // The following is used to enhanced simulation, to allows the networkManager
    // to quickly queue operations to client
    // When the client finishes a task it checks if other are available
    private final Deque<Object> pending;

    // ------------- for logging purposes ------------- //
    private final AsyncRunLogger logger = AsyncRunLogger.get();
    private long startTime;
    private final ApplicationConfig.Replication replicationParameters;
    // ------------------------------------------------ //

    public Client(ApplicationConfig.Delays delaysParameters, ApplicationConfig.Replication replicationParameters) {
        this.delaysParameters = delaysParameters;
        this.replicationParameters = replicationParameters;
        opCounter = 0;
        busy = false;
        pending = new ArrayDeque<>();
    }

    static public Props props(ApplicationConfig.Delays delaysParameters, ApplicationConfig.Replication replicationParameters) {
        return Props.create(Client.class, () -> new Client(delaysParameters, replicationParameters));
    }

    /**
     * Required messages for the assignment
     * If the client is busy it will fail
     * @param startUpdateMsg
     */
    private void onStartUpdateMsg(Messages.StartUpdateMSg startUpdateMsg) {
        if (busy) {
            // console log error
            System.out.printf(
                    "[Client %s] Operation not started: %s%n",
                    getSelf().path().name(), "CLIENT BUSY"
            );
            return;
        }
        onStartOperation();
        busy = true;
        opCounter++;

        Messages.UpdateRequestMsg requestMsg = new Messages.UpdateRequestMsg(
                startUpdateMsg.dataKey,
                startUpdateMsg.value
        );
        sendNetworkDelayedMessage(getSelf(), startUpdateMsg.node, requestMsg);

        // I PUT TIMEOUT DUE TO POSSIBILITY TO CONTACT CRASHED NODE
        operationTimer = scheduleTimeout();
    }

    /**
     * Required messages for the assignment.
     * If the client is busy it will fail
     * @param startGetMsg
     */
    private void onStartGetMsg(Messages.StartGetMsg startGetMsg) {
        if (busy) {
            // console log error
            System.out.printf(
                    "[Client %s] Operation not started: %s%n",
                    getSelf().path().name(), "CLIENT BUSY"
            );
            return;
        }
        onStartOperation();
        busy = true;
        opCounter++;

        Messages.GetRequestMsg requestMsg = new Messages.GetRequestMsg(
                startGetMsg.dataKey
        );
        sendNetworkDelayedMessage(getSelf(), startGetMsg.node, requestMsg);

        // I PUT TIMEOUT DUE TO POSSIBILITY TO CONTACT CRASHED NODE
        operationTimer = scheduleTimeout();
    }

    private void onUpdateResultMsg(Messages.UpdateResultMsg updateResultMsg) {
        operationTimer.cancel();

        System.out.printf(
                "[Client %s] Update completed for dataKey=%d -> value=\"%s\" (new version=%d)%n",
                getSelf().path().name(), updateResultMsg.dataKey, updateResultMsg.value.getValue(), updateResultMsg.value.getVersion()
        );
        onEndOperation(updateResultMsg.operationUid,"UPDATE", updateResultMsg.dataKey, true, updateResultMsg.value.getVersion());
        busy = false;

        processNextIfIdle();
    }

    private void onGetResultMsg(Messages.GetResultMsg getResultMsg) {
        operationTimer.cancel();

        System.out.printf(
                "[Client %s] Get result: dataKey=%d -> value=\"%s\" (version=%d)%n",
                getSelf().path().name(), getResultMsg.dataKey, getResultMsg.value.getValue(), getResultMsg.value.getVersion()
        );
        onEndOperation(getResultMsg.operationUid,"GET", getResultMsg.dataKey, true, getResultMsg.value.getVersion());
        busy = false;

        processNextIfIdle();
    }

    private void onErrorMsg(Messages.ErrorMsg errorMsg) {
        operationTimer.cancel();

        System.out.printf(
                "[Client %s] Operation failed: %s%n",
                getSelf().path().name(), errorMsg.reason
        );
        onEndOperation(errorMsg.operationUid, errorMsg.operationType, errorMsg.dataKey, false, -1);
        busy = false;

        processNextIfIdle();
    }

    private void onTimeout(ClientTimeout timeout) {
        // stale timeout -> can happen if it fires as we cancel it
        if (!busy) {
            return;
        }

        // release
        busy = false;

        System.out.printf(
                "[Client %s] TIMEOUT! Coordinator did not respond.%n",
                getSelf().path().name()
        );
    }

    // Always enqueue (for simulation purposes)
    private void onQueueUpdate(Messages.QueueUpdateMsg queueUpdateMsg) {
        pending.addLast(new Messages.StartUpdateMSg(queueUpdateMsg.dataKey, queueUpdateMsg.value, queueUpdateMsg.node));

        // if this is the first op I want to start it
        processNextIfIdle();
    }
    private void onQueueGet(Messages.QueueGetMsg queueGetMsg) {
        pending.addLast(new Messages.StartGetMsg(queueGetMsg.dataKey, queueGetMsg.node));

        // if this is the first op I want to start it
        processNextIfIdle();
    }

    // Start the next queued operation if idle
    private void processNextIfIdle() {
        if (!busy && !pending.isEmpty()) {
            Object next = pending.pollFirst();
            startOperation(next);
        }
    }

    // Actually send the request to the chosen coordinator
    private void startOperation(Object intent) {
        if (intent instanceof Messages.StartUpdateMSg m) {
            busy = true; opCounter++;
            Messages.UpdateRequestMsg req = new Messages.UpdateRequestMsg(m.dataKey, m.value);
            m.node.tell(req, getSelf());
            System.out.printf("[Client %s][op %d] -> update(%d, \"%s\")%n",
                    getSelf().path().name(), opCounter, m.dataKey, m.value);
        } else if (intent instanceof Messages.StartGetMsg m) {
            busy = true; opCounter++;
            Messages.GetRequestMsg req = new Messages.GetRequestMsg(m.dataKey);
            m.node.tell(req, getSelf());
            System.out.printf("[Client %s][op %d] -> get(%d)%n",
                    getSelf().path().name(), opCounter, m.dataKey);
        } else {
            System.out.printf("[Client %s] !! Unknown intent type: %s%n",
                    getSelf().path().name(), intent.getClass().getSimpleName());
        }
    }

    private Cancellable scheduleTimeout() {
        // Avoid to fire
        if (operationTimer!= null && !operationTimer.isCancelled()) {
            operationTimer.cancel();
        }
        final long safetyTimeForExecution = 1000L;
        final long timeoutTime = replicationParameters.T + (delaysParameters.delayMaxMs * 2) + safetyTimeForExecution;
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(timeoutTime, TimeUnit.MILLISECONDS),
                getSelf(),
                new ClientTimeout(),
                getContext().system().dispatcher(),
                getSelf());
    }

    private class ClientTimeout implements Serializable {}

    /**
     * Send message with network delays
     * @param sender
     * @param receiver
     * @param message
     */
    private void sendNetworkDelayedMessage(ActorRef sender, ActorRef receiver, Serializable message) {
        long delayMs = ThreadLocalRandom.current().nextLong(delaysParameters.delayMinMs, delaysParameters.delayMaxMs);
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delayMs, TimeUnit.MILLISECONDS),
                receiver,
                message,
                getContext().system().dispatcher(),
                sender
        );
    }

    // -------------- HELPER FUNCTION FOR LOGGING -------------- //
    private void onStartOperation() {
        this.startTime = System.nanoTime();
    }
    private void onEndOperation(OperationUid operationUid, String operationType, int dataKey, boolean success, int chosenVersion) {
        long endTime = System.nanoTime();
        var s = new LogModels.Summary(
                Instant.ofEpochMilli(this.startTime/1_000_000).toString(),
                Instant.ofEpochMilli(endTime/1_000_000).toString(),
                operationUid.toString(),
                getSender().path().name(),
                operationType,
                dataKey,
                chosenVersion,
                success,
                (endTime - this.startTime) / 1_000_000,
                "",
                replicationParameters.N, replicationParameters.R, replicationParameters.W, replicationParameters.T
        );
        logger.summary(s);
    }
    // --------------------------------------------------------- //


    // Mapping between the received message types and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.StartUpdateMSg.class, this::onStartUpdateMsg)
                .match(Messages.StartGetMsg.class, this::onStartGetMsg)
                // Convenience enqueuers
                .match(Messages.QueueUpdateMsg.class, this::onQueueUpdate)
                .match(Messages.QueueGetMsg.class, this::onQueueGet)
                // Result
                .match(Messages.UpdateResultMsg.class, this::onUpdateResultMsg)
                .match(Messages.GetResultMsg.class, this::onGetResultMsg)
                .match(Messages.ErrorMsg.class, this::onErrorMsg)
                .match(ClientTimeout.class, this::onTimeout)
                .build();
    }
}
