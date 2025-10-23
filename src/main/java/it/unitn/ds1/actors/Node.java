package it.unitn.ds1.actors;

import akka.actor.*;
import it.unitn.ds1.Messages;

import java.util.*;

public class Node extends AbstractActor {

//    private final ActorRef networkManager;
    private final Map<Integer, ActorRef> network;
    private final int id;

    public Node(
//            ActorRef networkManager,
            int id){
//        this.networkManager = networkManager;
        this.network = new TreeMap<>();
        this.id = id;
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

    // Mapping between the received message types and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.JoinNetworkMsg.class, this::onJoinNetworkMsg)
                .build();
    }
}
