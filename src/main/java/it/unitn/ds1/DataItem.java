package it.unitn.ds1;

import java.io.Serializable;

public class DataItem implements Serializable {
    private final String value;
    private final int version;

    public DataItem(String value, int version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() {
        return value;
    }
    public Integer getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "DataItem{" + "value=" + value + ", version=" + version + '}';
    }
}
