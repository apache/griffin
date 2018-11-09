package org.apache.griffin.core.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class GriffinEventManager {
    @Autowired
    private ApplicationContext applicationContext;

    @Value("#{'${internal.event.listeners}'.split(',')}")
    private Set<String> enabledListeners;

    private List<GriffinHook> eventListeners;

    @PostConstruct
    void initializeListeners() {
        List<GriffinHook> eventListeners = new ArrayList<>();
        applicationContext.getBeansOfType(GriffinHook.class)
                .forEach((beanName, listener) -> {
                    if (enabledListeners.contains(beanName)) {
                        eventListeners.add(listener);
                    }
                });
        this.eventListeners = eventListeners;
    }

    public void notifyListeners(GriffinEvent event) {
        eventListeners.forEach(listener -> {
            listener.onEvent(event);
        });
    }
}
