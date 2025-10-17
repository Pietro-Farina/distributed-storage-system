package it.unitn.ds1;

public class DataItem {
    private String value;
    private Integer version;

    public DataItem(String value, Integer version) {
        this.value = value;
        this.version = version;
    }

    public String getValue() {
        return value;
    }
    public Integer getVersion() {
        return version;
    }

    public void setValue(String value) {
        this.value = value;
    }
    public void setVersion(Integer version) {
        this.version = version;
    }
}
