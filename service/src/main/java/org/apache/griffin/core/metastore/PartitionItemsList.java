package org.apache.griffin.core.metastore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PartitionItemsList implements Serializable {

    public List<PartitionItems> getItemsList() {
        return itemsList;
    }

    public void setItemsList(List<PartitionItems> itemsList) {
        this.itemsList = itemsList;
    }

    private List<PartitionItems> itemsList;

    public List<String> generateString() {
        List<String> strs = new ArrayList<String>();
        for (PartitionItems items : getItemsList()) {
            strs.add(items.generateString());
        }
        return strs;
    }

}
