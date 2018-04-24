
/**
 * Copyright (C) 2015-2017 The Apache Software Foundation and Expedia Inc.
 *
 * This code is based on Hive's HiveMetaStoreClient:
 *
 * https://github.com/apache/hive/blob/rel/release-2.1.0/metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.griffin.core.metastore.hive;

        import java.io.Closeable;
        import java.io.IOException;
        import java.net.URI;
        import java.util.Random;
        import java.util.concurrent.TimeUnit;
        import java.util.concurrent.atomic.AtomicInteger;

        import org.apache.hadoop.hive.conf.HiveConf;
        import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
        import org.apache.hadoop.hive.conf.HiveConfUtil;
        import org.apache.hadoop.hive.metastore.MetaStoreUtils;
        import org.apache.hadoop.hive.metastore.api.MetaException;
        import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
        import org.apache.hadoop.hive.shims.ShimLoader;
        import org.apache.hadoop.hive.shims.Utils;
        import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
        import org.apache.hadoop.util.StringUtils;
        import org.apache.thrift.TException;
        import org.apache.thrift.protocol.TBinaryProtocol;
        import org.apache.thrift.protocol.TCompactProtocol;
        import org.apache.thrift.protocol.TProtocol;
        import org.apache.thrift.transport.TFramedTransport;
        import org.apache.thrift.transport.TSocket;
        import org.apache.thrift.transport.TTransport;
        import org.apache.thrift.transport.TTransportException;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

class ThriftMetastoreClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftMetastoreClient.class);

    private static final AtomicInteger CONN_COUNT = new AtomicInteger(0);

    private ThriftHiveMetastore.Iface client = null;
    private TTransport transport = null;
    private boolean isConnected = false;
    private URI metastoreUris[];
    private String tokenStrForm;
    protected final HiveConf conf;

    // for thrift connects
    private int retries = 5;
    private long retryDelaySeconds = 0;

    public ThriftMetastoreClient(HiveConf conf) {
        this.conf = conf;
        String msUri = conf.getVar(ConfVars.METASTOREURIS);

        if (HiveConfUtil.isEmbeddedMetaStore(msUri)) {
            throw new RuntimeException("You can't waggle an embedded metastore");
        }

        // get the number retries
        retries = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES);
        retryDelaySeconds = conf.getTimeVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

        // user wants file store based configuration
        if (msUri != null) {
            String metastoreUrisString[] = msUri.split(",");
            metastoreUris = new URI[metastoreUrisString.length];
            try {
                int i = 0;
                for (String s : metastoreUrisString) {
                    URI tmpUri = new URI(s);
                    if (tmpUri.getScheme() == null) {
                        throw new IllegalArgumentException("URI: " + s + " does not have a scheme");
                    }
                    metastoreUris[i++] = tmpUri;
                }
            } catch (IllegalArgumentException e) {
                throw (e);
            } catch (Exception e) {
                String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
                LOG.error(exInfo, e);
                throw new RuntimeException(exInfo, e);
            }
        } else {
            LOG.error("NOT getting uris from conf");
            throw new RuntimeException("MetaStoreURIs not found in conf file");
        }
    }

    public void open() {
        if (isConnected) {
            return;
        }
        TTransportException tte = null;
        boolean useSasl = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
        boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
        boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
        int clientSocketTimeout = (int) conf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

        for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
            for (URI store : metastoreUris) {
                LOG.info("Trying to connect to metastore with URI " + store);
                try {
                    transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
                    if (useSasl) {
                        // Wrap thrift connection with SASL for secure connection.
                        try {
                            HadoopThriftAuthBridge.Client authBridge = ShimLoader.getHadoopThriftAuthBridge().createClient();

                            // check if we should use delegation tokens to authenticate
                            // the call below gets hold of the tokens if they are set up by hadoop
                            // this should happen on the map/reduce tasks if the client added the
                            // tokens into hadoop's credential store in the front end during job
                            // submission.
                            String tokenSig = conf.getVar(ConfVars.METASTORE_TOKEN_SIGNATURE);
                            // tokenSig could be null
                            tokenStrForm = Utils.getTokenStrForm(tokenSig);
                            if (tokenStrForm != null) {
                                // authenticate using delegation tokens via the "DIGEST" mechanism
                                transport = authBridge.createClientTransport(null, store.getHost(), "DIGEST", tokenStrForm, transport,
                                        MetaStoreUtils.getMetaStoreSaslProperties(conf));
                            } else {
                                String principalConfig = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
                                transport = authBridge.createClientTransport(principalConfig, store.getHost(), "KERBEROS", null,
                                        transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
                            }
                        } catch (IOException ioe) {
                            LOG.error("Couldn't create client transport", ioe);
                            throw new MetaException(ioe.toString());
                        }
                    } else if (useFramedTransport) {
                        transport = new TFramedTransport(transport);
                    }
                    final TProtocol protocol;
                    if (useCompactProtocol) {
                        protocol = new TCompactProtocol(transport);
                    } else {
                        protocol = new TBinaryProtocol(transport);
                    }
                    client = new ThriftHiveMetastore.Client(protocol);
                    try {
                        transport.open();
                        LOG.info("Opened a connection to metastore '"
                                + store
                                + "', total current connections to all metastores: "
                                + CONN_COUNT.incrementAndGet());

                        isConnected = true;
                    } catch (TTransportException e) {
                        tte = e;
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("Failed to connect to the MetaStore Server...", e);
                        } else {
                            // Don't print full exception trace if DEBUG is not on.
                            LOG.warn("Failed to connect to the MetaStore Server...");
                        }
                    }
                } catch (MetaException e) {
                    LOG.error("Unable to connect to metastore with URI " + store + " in attempt " + attempt, e);
                }
                if (isConnected) {
                    break;
                }
            }
            // Wait before launching the next round of connection retries.
            if (!isConnected && retryDelaySeconds > 0) {
                try {
                    LOG.info("Waiting " + retryDelaySeconds + " seconds before next connection attempt.");
                    Thread.sleep(retryDelaySeconds * 1000);
                } catch (InterruptedException ignore) {}
            }
        }

        if (!isConnected) {
            throw new RuntimeException("Could not connect to meta store using any of the URIs provided. Most recent failure: "
                    + StringUtils.stringifyException(tte));
        }
        LOG.info("Connected to metastore.");
    }

    public void reconnect() {
        close();
        // Swap the first element of the metastoreUris[] with a random element from the rest
        // of the array. Rationale being that this method will generally be called when the default
        // connection has died and the default connection is likely to be the first array element.
        promoteRandomMetaStoreURI();
        open();
    }

    @Override
    public void close() {
        if (!isConnected) {
            return;
        }
        isConnected = false;
        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (TException e) {
            LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
        }
        // Transport would have got closed via client.shutdown(), so we don't need this, but
        // just in case, we make this call.
        if (isOpen()) {
            transport.close();
            transport = null;
        }
        LOG.info("Closed a connection to metastore, current connections: " + CONN_COUNT.decrementAndGet());
    }

    public boolean isOpen() {
        return transport != null && transport.isOpen();
    }

    protected ThriftHiveMetastore.Iface getClient() {
        return client;
    }

    /**
     * Swaps the first element of the metastoreUris array with a random element from the remainder of the array.
     */
    private void promoteRandomMetaStoreURI() {
        if (metastoreUris.length <= 1) {
            return;
        }
        Random rng = new Random();
        int index = rng.nextInt(metastoreUris.length - 1) + 1;
        URI tmp = metastoreUris[0];
        metastoreUris[0] = metastoreUris[index];
        metastoreUris[index] = tmp;
    }

}