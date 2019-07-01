//package org.apache.griffin.core.metastore.hive;
//
//import org.apache.griffin.core.config.CacheConfig;
//import org.apache.hadoop.hive.metastore.api.FieldSchema;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.TestConfiguration;
//import org.springframework.cache.CacheManager;
//import org.springframework.cache.annotation.EnableCaching;
//import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
//import org.springframework.context.annotation.Bean;
//import org.springframework.test.context.junit4.SpringRunner;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.List;
//
///**
// * Author: qwang8
// * Date:   2019-06-25.
// */
//@RunWith(SpringRunner.class)
//public class HiveMetastoreJDBCImplTest {
//
//    @TestConfiguration
//    @EnableCaching
//    public static class HiveMetaStoreServiceConfiguration extends CacheConfig {
//        @Bean("hiveMetaStoreServiceImpl")
//        public HiveMetaStoreService service() {
//            return new HiveMetaStoreServiceJDBCImpl();
//        }
//
//        @Bean
//        CacheManager cacheManager() {
//            return new ConcurrentMapCacheManager("hive");
//        }
//    }
//
//    @Autowired
//    private HiveMetaStoreServiceJDBCImpl serviceJDBC;
//
//    @Autowired
//    private HiveMetaStoreService service;
//
//    @Autowired
//    private CacheManager cacheManager;
//
//    private HiveConnectMgr hiveConnectMgr = HiveConnectMgr.getHiveConnectionMgr();
//
//    @Mock
//    private Connection conn;
//
//    @Mock
//    private Statement stmt;
//
//    @Mock
//    private ResultSet rs;
//
//    @Before
//    public void setup() {
//        cacheManager.getCache("hive").clear();
//    }
//
//    @Test
//    public void testParseMetadata() {
//        String meta = "CREATE EXTERNAL TABLE `default.merch_data`(  `merch_date` string COMMENT '',   `site_id` int COMMENT '',   `guid` string COMMENT '',   `user_id` string COMMENT '',   `treatments` string COMMENT '',   `experiments` string COMMENT '',   `page_id` int COMMENT '',   `placement_id` int COMMENT '',   `meid` string COMMENT '',   `program_id` bigint COMMENT '',   `module_id` int COMMENT '',   `algorithm_selected_config_id` bigint COMMENT '',   `algorithm_id` int COMMENT '',   `mbe` string COMMENT '',   `issps` string COMMENT '',   `mbe_version` string COMMENT '',   `trks` string COMMENT '',   `session_skey` string COMMENT '',   `seqnum` string COMMENT '',   `channel` int COMMENT '',   `icf` string COMMENT '',   `device` string COMMENT '',   `device_exp` string COMMENT '',   `experience` string COMMENT '',   `euid` string COMMENT '',   `impression_count` bigint COMMENT '',   `surface_plmt_imp` bigint COMMENT '',   `plmt_imp` bigint COMMENT '',   `oi` int COMMENT '',   `in` string COMMENT '',   `seeditem` string COMMENT '',   `seeditem_v2` array<string> COMMENT '',   `multi_seeds` array<string> COMMENT '',   `out` array<string> COMMENT '',   `vi` array<string> COMMENT '',   `bid` array<string> COMMENT '',   `bin` array<string> COMMENT '',   `offer` array<string> COMMENT '',   `watch` array<string> COMMENT '',   `asq` array<string> COMMENT '',   `add2cart` array<string> COMMENT '',   `add2list` array<string> COMMENT '',   `purchase_items` array<string> COMMENT '',   `purchase` bigint COMMENT '',   `migmb` bigint COMMENT '',   `mbe_value` string COMMENT '',   `out_deals` string COMMENT '',   `collection` array<string> COMMENT '',   `lndpg_click` array<string> COMMENT '')COMMENT 'merch_data for merch team'PARTITIONED BY (   `dt` string,   `placement` int)ROW FORMAT SERDE   'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'LOCATION  'hdfs://ares-lvs-nn-ha/apps/hdmi-set/buyexp/merch/common/merch_data'TBLPROPERTIES (  'COLUMN_STATS_ACCURATE'='false',   'avro.schema.url'='hdfs://ares-lvs-nn-ha/user/b_bis/merch/avro/merch-data-1.0.avsc',   'transient_lastDdlTime'='1535651637')";
//        System.out.println(serviceJDBC.getLocation(meta));
//
//        List<FieldSchema> cols = serviceJDBC.getColums(meta);
//        for (FieldSchema fs : cols) {
//            System.out.println(fs);
//        }
//    }
//}
