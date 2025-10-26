package it.unitn.ds1.protocol;

import it.unitn.ds1.DataItem;

import java.util.*;

public class QuorumTracker {
    // TODO
    private Set<Integer> expected;   // usually all N replicas for the dataKey
    private int requiredQuorum;

    private final Set<Integer> oks;
    private final Set<Integer> busy;

    public QuorumTracker(Set<Integer> expected, int requiredQuorum) {
        this.expected = Set.copyOf(expected);
        this.requiredQuorum = requiredQuorum;
        this.oks = new HashSet<>();
        this.busy = new HashSet<>();
    }

    public void onOk(int nodeKey) {
        oks.add(nodeKey);
    }

    public void onBusy(int nodeKey) {
        busy.add(nodeKey);
    }

    public boolean hasQuorum() {
        return oks.size() >= requiredQuorum;
    }

    // if we have enough vote to end the voting part: either we have quorum or we can't have it anymore
    public boolean done() {
        return oks.size() >=  requiredQuorum ||
                oks.size() + voteMissing() < requiredQuorum;
    }

    // How many votes are missing
    private int voteMissing() {
        return expected.size() - (oks.size() + busy.size());
    }

    // To allow joining operations
    public void setQuorumRequirements(Set<Integer> expected, int requiredQuorum) {
        this.expected = Set.copyOf(expected);
        this.requiredQuorum = requiredQuorum;
    }
}
