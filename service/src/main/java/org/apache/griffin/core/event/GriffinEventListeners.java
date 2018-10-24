package org.apache.griffin.core.event;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class GriffinEventListeners {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(GriffinEventListeners.class);

    @Autowired
    private ApplicationContext context;
    @Value("#{'${internal.event.listeners}'.split(',')}")
    private List<String> listeners;

    @Bean
    public List<GriffinHook> getListenersRegistered() {
        ArrayList<GriffinHook> hookList = new ArrayList<>();
        if (listeners == null || listeners.isEmpty()) {
            LOGGER.info("Disable griffin event listener for service.");
        } else {
            listeners.forEach(name -> {
                GriffinHook bean = null;
                try {
                    bean = (GriffinHook) context.getBean(name);
                    hookList.add(bean);
                } catch (BeansException e) {
                    LOGGER.error("Fail to load griffin hook bean {}, due to {}",
                            name, e.getMessage());
                }
            });
        }
        return hookList;
    }

    @Bean
    public GriffinEventManager getGriffinEventManager() {
        return new GriffinEventManager();
    }
}
