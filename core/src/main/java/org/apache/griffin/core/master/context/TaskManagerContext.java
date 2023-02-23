package org.apache.griffin.core.master.context;

import com.google.common.collect.Maps;
import org.apache.griffin.core.master.transport.DQCConnection;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class TaskManagerContext {

    public static final Map<Long, DQCConnection> submitTaskContainer = Maps.newConcurrentMap();

    public void addTask(Long instanceId, DQCConnection client) {
        submitTaskContainer.put(instanceId, client);
    }

    public void clearTask(Long instanceId) {
        submitTaskContainer.remove(instanceId);
    }
}
