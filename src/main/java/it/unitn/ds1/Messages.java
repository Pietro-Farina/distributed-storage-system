package it.unitn.ds1;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.*;

public class Messages {

    public static class JoinNetworkMsg implements Serializable {
        public final Map<Integer, ActorRef> network;

        public JoinNetworkMsg(Map<Integer, ActorRef> network) {
            this.network = Collections.unmodifiableMap(new TreeMap<>(network));
        }
    }
}
