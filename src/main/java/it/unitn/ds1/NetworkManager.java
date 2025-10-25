package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Client;
import it.unitn.ds1.actors.Node;
import it.unitn.ds1.protocol.Messages;
import it.unitn.ds1.utils.ApplicationConfig;

import java.util.*;

public class NetworkManager {
    private final ActorSystem system;
    private final NavigableMap<Integer, ActorRef> network;
    private final List<ActorRef> clients;

    // replication parameters -> Gave to the
    public final ApplicationConfig parameters;

    public NetworkManager(ApplicationConfig parameters) {
        system = ActorSystem.create("distributed-storage-system");
        network = new TreeMap<>();
        clients = new ArrayList<>();
        this.parameters = parameters;
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
            clients.add(system.actorOf(Client.props(parameters.delays), "client" + ++i));
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

    public void terminate() {
        system.terminate();
    }

}
