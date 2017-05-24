package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"hive.metastore.uris=thrift://10.9.246.187:9083"})
public class HiveMetastoreServiceTests {

    @TestConfiguration
    public static class HiveMetastoreServiceConfiguration{
        @Bean
        public HiveMetastoreService service(){
            return new HiveMetastoreService();
        }

        @Value("${hive.metastore.uris}")
        String urls;
        @Bean
        public HiveMetaStoreClient client(){

                HiveMetaStoreClient client = null;
                HiveConf hiveConf = new HiveConf();
                hiveConf.set("hive.metastore.local", "false");
                hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
                hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, urls);
                try {
                    client= new HiveMetaStoreClient(hiveConf);
                } catch (MetaException e) {
                    client = null;
                }

                return client;

        }
    }
    @Autowired private HiveMetastoreService service;


    @Before
    public void setup(){

    }

    @Test
    public void testGetAllTables(){
        Iterable<String> tmp = service.getAllDatabases();
        List<String> results = new ArrayList<String>();
        for(String t : tmp){
            results.add(t);
            System.out.println(t);
        }
        assertThat(results.size()).isGreaterThan(1);
    }
}
