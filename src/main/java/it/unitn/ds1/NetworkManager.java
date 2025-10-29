package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Node;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;

import java.util.*;

public class NetworkManager {
    private final ActorSystem system;
    public final NavigableMap<Integer, ActorRef> network;
    public final List<ActorRef> clients;
    private final ActorRef inbox;

    // replication parameters -> Gave to the
    public final ApplicationConfig parameters;

    public NetworkManager(ApplicationConfig parameters) {
        system = ActorSystem.create("distributed-storage-system");
        network = new TreeMap<>();
        clients = new ArrayList<>();
        this.parameters = parameters;
        this.inbox = system.actorOf(ManagerInbox.props(this), "networkManagerInbox");
    }

    /**
     * NOTE the entries added to the network might not be deterministic
     * @param nodeKeysToAdd
     * @param dataToAdd
     */
    public void initializeNetwork(List<Integer> nodeKeysToAdd, Map<Integer, String> dataToAdd, boolean deterministic) {
        // Add nodes to the system and create the same numbers of client (for bulk updates)
        int i = 0;
        for (Integer nodeKey : nodeKeysToAdd) {
            network.put(nodeKey, system.actorOf(Node.props(nodeKey, parameters.replication, parameters.delays), "node_" + nodeKey));
            clients.add(system.actorOf(Client.props(parameters.delays, parameters.replication), "client" + ++i));
        }

        // Send join messages to the nodes to inform them of the whole network
        Messages.JoinNetworkMsg start = new Messages.JoinNetworkMsg(network);
        for (Map.Entry<Integer, ActorRef> peer : network.entrySet()) {
            peer.getValue().tell(start, ActorRef.noSender());
        }

        if (dataToAdd.isEmpty() || clients.isEmpty()) return;

        final int nClients = clients.size();

        List<Map.Entry<Integer, String>> entries = new ArrayList<>(dataToAdd.entrySet());

        for (int idx = 0; idx < entries.size(); idx++) {
            Map.Entry<Integer, String> e = entries.get(idx);
            int key = e.getKey();
            String value = e.getValue();

            ActorRef client;
            ActorRef coordinator;
            if (deterministic) {
                client = clients.getFirst();
                coordinator = network.get(nodeKeysToAdd.getFirst());
            } else {
                client = clients.get(idx % nClients);
                coordinator = network.get(nodeKeysToAdd.get(idx % nClients));
            }
            client.tell(new Messages.QueueUpdateMsg(key, value, coordinator), ActorRef.noSender());
        }

        if (deterministic) {
            // (Optional) Log a quick summary
            System.out.printf("Dispatched %d updates across %d clients (%d each, remainder %d).%nOperations might require some times%n",
                    entries.size(), 1, entries.size(), 0);
        } else {
            // (Optional) Log a quick summary
            System.out.printf("Dispatched %d updates across %d clients (%d each, remainder %d).%nOperations might require some times%n",
                    entries.size(), nClients, entries.size() / nClients, entries.size() % nClients);
        }
    }

    public void addNode(int newNodeKey, int bootstrapNodeKey) {
        if (network.containsKey(newNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "JOIN", newNodeKey, "node already exists in network"
            );
            return;
        }
        if (!network.containsKey(bootstrapNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "JOIN", newNodeKey, "bootstrap node not in network"
            );
            return;
        }
        ActorRef newNode = system.actorOf(Node.props(newNodeKey, parameters.replication, parameters.delays), "node_" + newNodeKey);
        newNode.tell(new Messages.StartJoinMsg(newNodeKey, network.get(bootstrapNodeKey)), ActorRef.noSender());
    }

    public void removeNode(int nodeKey) {
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "LEAVE", nodeKey, "node not in network"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.StartLeaveMsg(), ActorRef.noSender());
    }

    public void crashNode(int nodeKey) {
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "CRASH", nodeKey, "node not in network"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.CrashMsg(), ActorRef.noSender());
    }

    public void recoverNode(int nodeKey, int bootstrapNodeKey) {
        if (!network.containsKey(nodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", nodeKey, "node not in network"
            );
            return;
        }
        if (!network.containsKey(bootstrapNodeKey)) {
            System.out.printf(
                    "[Network Manager] %s Operation for nodeKey=%d failed: %s%n",
                    "RECOVER", bootstrapNodeKey, "bootstrap node [" + bootstrapNodeKey +"] not in network"
            );
            return;
        }
        ActorRef node = network.get(nodeKey);
        node.tell(new Messages.StartRecoveryMsg(network.get(bootstrapNodeKey)), ActorRef.noSender());
    }



    public void terminate() {
        system.terminate();
    }

    // ------------ HELPERS FOR HAVING UPDATED RING ON DATA MANAGER ------------ //
    /**
     * The following functions are implemented as follows since we are under the assumptions that
     * "Nodes join and leave, crash and recover one at a time, and only when there are no ongoing operations".
     * If it wasn't for that, then this implementation could lead to inconsistency due to race conditions.
     */

    // called only by the inbox actor (single-threaded), but keep synchronized for safety
    synchronized void onJoin(int key, ActorRef ref) { network.put(key, ref); }
    synchronized void onLeave(int key) { network.remove(key); }
    synchronized void onCrash(int key) { /* keep membership; optionally track a status map */ }
    synchronized void onRecover(int key, ActorRef ref) { network.put(key, ref); }

    // for your helpers/UI/tests
    public synchronized NavigableMap<Integer, ActorRef> snapshot(){ return new TreeMap<>(network); }

    public static class ManagerInbox extends AbstractActor {
        private final NetworkManager networkManager;

        private ManagerInbox(NetworkManager networkManager) {
            this.networkManager = networkManager;
        }
        public static Props props(NetworkManager networkManager) {
            return Props.create(ManagerInbox.class, () -> new ManagerInbox(networkManager));
        }

        @Override public Receive createReceive(){
            return receiveBuilder()
                    .match(Messages.ManagerNotifyJoin.class,m -> networkManager.onJoin(m.nodeKey, sender()))
                    .match(Messages.ManagerNotifyLeave.class,m -> networkManager.onLeave(m.nodeKey))
                    .match(Messages.ManagerNotifyCrash.class,m -> networkManager.onCrash(m.nodeKey))
                    .match(Messages.ManagerNotifyRecover.class,m -> networkManager.onRecover(m.nodeKey, sender()))
                    .build();
        }
    }
    // ------------------------------------------------------------------------- //
}
