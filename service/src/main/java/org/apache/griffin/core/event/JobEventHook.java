package org.apache.griffin.core.event;

import org.apache.griffin.core.exception.GriffinException;
import org.springframework.context.annotation.Configuration;

@Configuration(value = "GriffinJobEventHook")
public class JobEventHook implements GriffinHook {
    @Override
    public void onEvent(GriffinEvent event) throws GriffinException {
        // This method needs to be reimplemented by event-consuming purpose
    }
}
