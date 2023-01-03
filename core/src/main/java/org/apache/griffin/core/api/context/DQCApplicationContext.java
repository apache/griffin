package org.apache.griffin.core.api.context;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master Runtime Env
 * Scope: Singleton
 */
@Component
@Data
public class DQCApplicationContext {
    private Map<String, WorkerContext> context = new ConcurrentHashMap<>();
}
