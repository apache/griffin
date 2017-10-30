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

package org.apache.griffin.core.metastore.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class HiveMetaStoreProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetaStoreProxy.class);

    @Value("${hive.metastore.uris}")
    private String uris;

    /**
     * Set attempts and interval for HiveMetastoreClient to retry.
     *
     * @hive.hmshandler.retry.attempts: The number of times to retry a HMSHandler call if there were a connection error.
     * @hive.hmshandler.retry.interval: The time between HMSHandler retry attempts on failure.
     */
    @Value("${hive.hmshandler.retry.attempts}")
    private int attempts;

    @Value("${hive.hmshandler.retry.interval}")
    private String interval;

    private HiveMetaStoreClient client = null;

    @Bean
    public HiveMetaStoreClient initHiveMetastoreClient() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, uris);
        hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, attempts);
        hiveConf.setVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, interval);
        try {
            client = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            LOGGER.error("Failed to connect hive metastore. {}", e.getMessage());
            client = null;
        }

        return client;
    }

    @PreDestroy
    public void destroy() throws Exception {
        if (null != client) {
            client.close();
        }
    }
}
