package org.apache.griffin.core.metastore;

import org.apache.griffin.core.GriffinWebApplication;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
public class HiveMetastoreProxy
{
    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreProxy.class);

    @Value("${hive.metastore.uris}")
    private String uris;


    private HiveMetaStoreClient client = null;

    @Bean
    public HiveMetaStoreClient initHiveMetastoreClient(){
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, uris);
        try {
            client= new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            log.error("Failed to connect hive metastore",e.getMessage());
            client = null;
        }

        return client;
    }

    public void destroy() throws Exception {
        if(null!=client) client.close();
    }
}
