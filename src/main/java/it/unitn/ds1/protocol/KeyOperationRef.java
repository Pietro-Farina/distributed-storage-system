package it.unitn.ds1.protocol;

import it.unitn.ds1.utils.OperationUid;

import java.io.Serializable;

public record KeyOperationRef(int key, OperationUid opId) implements Serializable {
}