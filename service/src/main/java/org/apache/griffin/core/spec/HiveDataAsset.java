package org.apache.griffin.core.spec;

import javax.persistence.*;

/**
 * DataAsset based on hive metadata.
 */

@Entity
@Inheritance(
        strategy = InheritanceType.SINGLE_TABLE
)

public class HiveDataAsset implements DataAsset {
    public String getName() {
        return name;
    }

    public String getOrg() {
        return org;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner){
        this.owner = owner;
    }

    public void setOrg(String org){
        this.org = org;
    }

    public void setName(String name){
        this.name = name;
    }

    private String owner;
    private String org;
    @Id
    private String name;

    public HiveDataAsset(String name,String org,String owner){
        this.name = name;
        this.org = org;
        this.owner = owner;
    }

    public HiveDataAsset(){

    }
}
