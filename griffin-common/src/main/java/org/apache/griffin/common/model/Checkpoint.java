package org.apache.griffin.common.model;

public interface Checkpoint {
    void save(String checkpoint) throws Exception;
}
