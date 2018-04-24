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
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.security.CodeSource;

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

//    private HiveMetaStoreClient client = null;
    private ThriftMetastoreClient client = null;

    private static final Logger l4j = LoggerFactory.getLogger(HiveConf.class);

    private static URL checkConfigFile(File f) {
        try {
            return (f.exists() && f.isFile()) ? f.toURI().toURL() : null;
        } catch (Throwable e) {
            if (l4j.isInfoEnabled()) {
                l4j.info("Error looking for config " + f, e);
            }
            System.err.println("Error looking for config " + f + ": " + e.getMessage());
            return null;
        }
    }

    private static URL findConfigFile(ClassLoader classLoader, String name, boolean doLog) {
        URL result = classLoader.getResource(name);
        LOGGER.warn("result: {}", result);
        if (result == null) {
            String confPath = System.getenv("HIVE_CONF_DIR");
            LOGGER.warn("confPath: {}", confPath);
            result = checkConfigFile(new File(confPath, name));
            LOGGER.warn("result: {}", result);
            if (result == null) {
                String homePath = System.getenv("HIVE_HOME");
                LOGGER.warn("homePath: {}", homePath);
                String nameInConf = "conf" + File.pathSeparator + name;
                LOGGER.warn("nameInConf: {}", nameInConf);
                result = checkConfigFile(new File(homePath, nameInConf));
                LOGGER.warn("result: {}", result);
                if (result == null) {
                    URI jarUri = null;
                    try {
                        java.security.ProtectionDomain domain = HiveConf.class.getProtectionDomain();
                        LOGGER.warn("domain: {}", domain);
                        CodeSource codeSource = domain.getCodeSource();
                        LOGGER.warn("codeSource: {}", codeSource);
                        URL location = codeSource.getLocation();
                        LOGGER.warn("location: {}", location);
                        jarUri = location.toURI();

//                        jarUri = HiveConf.class.getProtectionDomain().getCodeSource().getLocation().toURI();
                        LOGGER.warn("jarUri: {}", jarUri);
                    } catch (Throwable e) {
                        if (l4j.isInfoEnabled()) {
                            l4j.info("Cannot get jar URI", e);
                        }
                        System.err.println("Cannot get jar URI: " + e.getMessage());
                    }
                    File f1 = new File(jarUri);
                    LOGGER.warn("f1: {}", f1);
                    File f2 = f1.getParentFile();
                    LOGGER.warn("f2: {}", f2);
                    File f3 = new File(f2, nameInConf);
                    LOGGER.warn("f3: {}", f3);
                    result = checkConfigFile(f3);
                }
            }
        }
        if (doLog && l4j.isInfoEnabled()) {
            l4j.info("Found configuration file " + result);
        }
        return result;
    }

//    @Bean
    public ThriftMetastoreClient initHiveMetastoreClient() {
        LOGGER.warn("begin client");
        LOGGER.warn(File.pathSeparator);
        LOGGER.warn(uris);

//        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader = HiveConf.class.getClassLoader();
        LOGGER.warn("classLoader: {}", classLoader);
        if (classLoader == null) {
            classLoader = HiveConf.class.getClassLoader();
            LOGGER.warn("classLoader: {}", classLoader);
        }
        URL url = findConfigFile(classLoader, "hive-site.xml", true);
        LOGGER.warn("url: {}", url);

        HiveConf hiveConf = new HiveConf();
        LOGGER.warn("hive conf success");
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, uris);
        hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, attempts);
        hiveConf.setVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, interval);
        try {
            LOGGER.warn("begin hive ms client");
            client = new ThriftMetastoreClient(hiveConf);
            LOGGER.warn("hive ms client success");
        } catch (Exception e) {
            LOGGER.error("Failed to connect hive metastore. {}", e.getMessage());
            client = null;
        }

        return client;
    }

    @PreDestroy
    public void destroy() {
        if (null != client) {
            client.close();
        }
    }
}
