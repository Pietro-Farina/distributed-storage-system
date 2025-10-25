package it.unitn.ds1.utils;

import java.io.Serializable;

/**
 * Represent an UID of an operation
 * Each UID is unique across the network and is composed by: {coordinator's id}-{coordinator's operationNumber}
 * Since for each operation the counter is increased and we cannot have more nodes with the same id,
 * the resulting operation ID will be unique
 * @return
 */
public record OperationUid(int coordinatorId, int opCounter) implements Serializable {
    @Override
    public String toString() {
        return coordinatorId + "-" + opCounter;
    }

    public static OperationUid parse(String s) {
        String[] p = s.split("-");
        if (p.length != 2) throw new IllegalArgumentException("Invalid UID format: " + s);
        return new OperationUid(Integer.parseInt(p[0]), Integer.parseInt(p[1]));
    }
}