package org.apache.griffin.common;

public interface Lifecycle {
    void start();
    void end();
    String getLifecycleStatus();
}
