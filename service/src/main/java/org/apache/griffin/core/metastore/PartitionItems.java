package org.apache.griffin.core.metastore;

import java.io.Serializable;
import java.util.List;

public class PartitionItems implements Serializable {

    public List<PartitionItem> getItems() {
        return items;
    }

    public void setItems(List<PartitionItem> items) {
        this.items = items;
    }

    private List<PartitionItem> items;

    public String generateString() {
        String s = "";
        int sz = items.size();
        for (int i = 0; i < sz; i++) {
            PartitionItem item = items.get(i);
            if (i < sz - 1) {
                s += item.generateString() + "/";
            } else {
                s += item.generateString();
            }
        }
        return s;
    }

}
