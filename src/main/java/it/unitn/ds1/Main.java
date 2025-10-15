package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actors.Node;

import java.util.TreeMap;
import java.util.Map;

public class Main {
    final static int N_NODES = 4;

    public static void main(String[] args) {

        final ActorSystem system = ActorSystem.create("distributed-storage-system");

        // Create nodes and put them to a list
        Map<Integer, ActorRef> network = new TreeMap<>();
        for (int i = 0; i < N_NODES; i++) {
            int id = i + 10;
            network.put(id, system.actorOf(Node.props(id), "node" + id));
        }

        // Send join messages to the nodes to inform them of the whole network
        Messages.JoinNetworkMsg start = new Messages.JoinNetworkMsg(network);
        for (Map.Entry<Integer, ActorRef> peer : network.entrySet()) {
            peer.getValue().tell(start, ActorRef.noSender());
        }

    }
}
