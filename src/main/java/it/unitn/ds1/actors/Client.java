package it.unitn.ds1.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.ds1.protocol.Messages;

import java.util.ArrayDeque;
import java.util.Deque;

/*
    The clients execute commands passed through messages, print the reply from the
    coordinator, and only then accept further commands. A client may perform multiple
    read and write operations
 */
public class Client extends AbstractActor {
    private int opCounter;
    private boolean busy;

    // The following is used to enhanced simulation, to allows the networkManager
    // to quickly queue operations to client
    // When the client finishes a task it checks if other are available
    private final Deque<Object> pending;

    public Client() {
        opCounter = 0;
        busy = false;
        pending = new ArrayDeque<>();
    }

    static public Props props(){
        return Props.create(Client.class, Client::new);
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
                    "[Client %s] ❌ Operation not started: %s%n",
                    getSelf().path().name(), "CLIENT BUSY"
            );
            return;
        }
        busy = true;
        opCounter++;

        Messages.UpdateRequestMsg requestMsg = new Messages.UpdateRequestMsg(
                startUpdateMsg.dataKey,
                startUpdateMsg.value
        );
        startUpdateMsg.node.tell(requestMsg, getSelf());

        // I DON'T PUT TIMEOUT BECAUSE WE ARE UNDER ASSUMPTIONS OF RELIABLE NETWORK
        // Otherwise we would have a timeout to avoid being indefinitely busy
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
                    "[Client %s] ❌ Operation not started: %s%n",
                    getSelf().path().name(), "CLIENT BUSY"
            );
            return;
        }
        busy = true;
        opCounter++;

        Messages.GetRequestMsg requestMsg = new Messages.GetRequestMsg(
                startGetMsg.dataKey
        );
        startGetMsg.node.tell(requestMsg, getSelf());

        // I DON'T PUT TIMEOUT BECAUSE WE ARE UNDER ASSUMPTIONS OF RELIABLE NETWORK
        // Otherwise we would have a timeout to avoid being indefinitely busy
    }

    private void onUpdateResultMsg(Messages.UpdateResultMsg updateResultMsg) {
        System.out.printf(
                "[Client %s] Update completed for key=%d -> value=\"%s\" (new version=%d)%n",
                getSelf().path().name(), updateResultMsg.dataKey, updateResultMsg.value.getValue(), updateResultMsg.value.getVersion()
        );
        busy = false;

        processNextIfIdle();
    }

    private void onGetResultMsg(Messages.GetResultMsg getResultMsg) {
        System.out.printf(
                "[Client %s] Get result: key=%d → value=\"%s\" (version=%d)%n",
                getSelf().path().name(), getResultMsg.dataKey, getResultMsg.value.getValue(), getResultMsg.value.getVersion()
        );
        busy = false;

        processNextIfIdle();
    }

    private void onErrorMsg(Messages.ErrorMsg errorMsg) {
        System.out.printf(
                "[Client %s] Operation failed: %s%n",
                getSelf().path().name(), errorMsg.reason
        );
        busy = false;

        processNextIfIdle();
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
                .build();
    }
}
