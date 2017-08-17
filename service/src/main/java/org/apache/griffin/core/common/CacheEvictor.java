package org.apache.griffin.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class CacheEvictor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheEvictor.class);


    @Scheduled(fixedRateString = "${cache.evict.hive.fixedRate}")
    @CacheEvict(cacheNames = "hive", allEntries = true, beforeInvocation = true)
    public void evictHiveCache() {
        LOGGER.info("Evict hive cache");
    }


}
