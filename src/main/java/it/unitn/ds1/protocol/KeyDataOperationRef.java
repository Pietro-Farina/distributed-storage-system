package it.unitn.ds1.protocol;

import it.unitn.ds1.DataItem;
import it.unitn.ds1.utils.OperationUid;

import java.io.Serializable;

public record KeyDataOperationRef (int dataKey, DataItem item, OperationUid opUid) implements Serializable {

}
