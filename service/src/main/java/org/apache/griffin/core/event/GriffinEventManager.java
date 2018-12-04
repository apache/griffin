/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

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
