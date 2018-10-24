package org.apache.griffin.core.event;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class GriffinEventManager {
    @Autowired
    private List<GriffinHook> eventListeners;

    public void notifyListeners(GriffinEvent event) {
        eventListeners.forEach(listener -> {
            listener.onEvent(event);
        });
    }
}
