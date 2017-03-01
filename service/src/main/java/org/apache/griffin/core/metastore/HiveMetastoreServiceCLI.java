package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.griffin.core.repo.HiveDataAssetRepo;
import org.apache.griffin.core.spec.HiveDataAsset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

@SpringBootApplication
public class HiveMetastoreServiceCLI implements CommandLineRunner{
    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreServiceCLI.class);
    public static void main(String[] args) {
        SpringApplication.run(HiveMetastoreServiceCLI.class, args);
    }

    @Autowired
    HiveMetastoreService hiveService;

    public void run(String... strings) throws Exception {
        {
            log.info(hiveService.toString());


        }
    }
}

//public class HiveMetastoreServiceCLI {
//    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreServiceCLI.class);
//
//    public static void main(String[] args) {
////        HiveMetastoreService hiveService = new HiveMetastoreService();
//        try {
//            HiveMetaStoreClient client = HiveMetastoreService.getHiveMetastoreClient("thrift://10.9.246.187:9083");
//            Partition part = client.getPartition("default", "test_partition", "dt=2017-02-02/hour=14");
//
////            Partition part = client.getPartition("default", "test_partition", "dt=2017-02-02/hour=14");
//            log.info(part.toString());
//
//        } catch(Exception e) {
//            e.printStackTrace();
//        }
//
//
//    }
//
//}
