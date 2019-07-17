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


import org.apache.griffin.core.config.CacheConfig;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;


@RunWith(SpringRunner.class)
public class HiveMetastoreServiceJDBCImplTest {

    @TestConfiguration
    @EnableCaching
    public static class HiveMetaStoreServiceConfiguration extends CacheConfig {
        @Bean("hiveMetaStoreServiceJdbcImpl")
        public HiveMetaStoreServiceJdbcImpl serviceJDBC() {
            return new HiveMetaStoreServiceJdbcImpl();
        }

        @Bean
        CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("jdbchive");
        }
    }

    private HiveMetaStoreServiceJdbcImpl serviceJDBC = new HiveMetaStoreServiceJdbcImpl();

    @Mock
    private Connection conn;

    @Mock
    private Statement stmt;

    @Mock
    private ResultSet rs;

    @Before
    public void setUp() throws SQLException {
        serviceJDBC.setConn(conn);
        serviceJDBC.setHiveClassName("org.apache.hive.jdbc.HiveDriver");
    }

    @Test
    public void testGetComment() {
        String colStr = "`merch_date` string COMMENT 'this is merch date'";
        String comment = serviceJDBC.getComment(colStr);
        assert (comment.equals("this is merch date"));

        colStr = "`merch_date` string COMMENT ''";
        comment = serviceJDBC.getComment(colStr);
        Assert.assertTrue(comment.isEmpty());
    }

    @Test
    public void testgetAllDatabases() throws SQLException {
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString(anyInt())).thenReturn("default");

        Iterable<String> res = serviceJDBC.getAllDatabases();
        for (String s : res) {
            Assert.assertEquals(s, "default");
            break;
        }
    }

    @Test
    public void testGetAllTableNames() throws SQLException {
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString(anyInt())).thenReturn("merch_data").thenReturn("merch_summary");

        Iterable<String> res = serviceJDBC.getAllTableNames("default");
        StringBuilder sb = new StringBuilder();
        for (String s : res) {
            sb.append(s).append(",");
        }
        Assert.assertEquals(sb.toString(), "merch_data,merch_summary,");
    }

    @Test
    public void testGetTable() throws SQLException {
        String meta = "CREATE EXTERNAL TABLE `default.merch_data`(  `merch_date` string COMMENT 'this is merch date',   `site_id` int COMMENT '',   `guid` string COMMENT '',   `user_id` string COMMENT '',   `treatments` string COMMENT '',   `experiments` string COMMENT '',   `page_id` int COMMENT '',   `placement_id` int COMMENT '',   `meid` string COMMENT '',   `program_id` bigint COMMENT '',   `module_id` int COMMENT '',   `algorithm_selected_config_id` bigint COMMENT '',   `algorithm_id` int COMMENT '',   `mbe` string COMMENT '',   `issps` string COMMENT '',   `mbe_version` string COMMENT '',   `trks` string COMMENT '',   `session_skey` string COMMENT '',   `seqnum` string COMMENT '',   `channel` int COMMENT '',   `icf` string COMMENT '',   `device` string COMMENT '',   `device_exp` string COMMENT '',   `experience` string COMMENT '',   `euid` string COMMENT '',   `impression_count` bigint COMMENT '',   `surface_plmt_imp` bigint COMMENT '',   `plmt_imp` bigint COMMENT '',   `oi` int COMMENT '',   `in` string COMMENT '',   `seeditem` string COMMENT '',   `seeditem_v2` array<string> COMMENT '',   `multi_seeds` array<string> COMMENT '',   `out` array<string> COMMENT '',   `vi` array<string> COMMENT '',   `bid` array<string> COMMENT '',   `bin` array<string> COMMENT '',   `offer` array<string> COMMENT '',   `watch` array<string> COMMENT '',   `asq` array<string> COMMENT '',   `add2cart` array<string> COMMENT '',   `add2list` array<string> COMMENT '',   `purchase_items` array<string> COMMENT '',   `purchase` bigint COMMENT '',   `migmb` bigint COMMENT '',   `mbe_value` string COMMENT '',   `out_deals` string COMMENT '',   `collection` array<string> COMMENT '',   `lndpg_click` array<string> COMMENT '')COMMENT 'merch_data for merch team'PARTITIONED BY (   `dt` string,   `placement` int)ROW FORMAT SERDE   'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT   'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT   'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'LOCATION  'hdfs://ares-lvs-nn-ha/apps/hdmi-set/buyexp/merch/common/merch_data'TBLPROPERTIES (  'COLUMN_STATS_ACCURATE'='false',   'avro.schema.url'='hdfs://ares-lvs-nn-ha/user/b_bis/merch/avro/merch-data-1.0.avsc',   'transient_lastDdlTime'='1535651637')";
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString(anyInt())).thenReturn(meta);

        Table res = serviceJDBC.getTable("default", "merch_data");

        assert (res.getDbName().equals("default"));
        assert (res.getTableName().equals("merch_data"));
        assert (res.getSd().getLocation().equals("hdfs://ares-lvs-nn-ha/apps/hdmi-set/buyexp/merch/common/merch_data"));
        List<FieldSchema> fieldSchemas = res.getSd().getCols();
        for (FieldSchema fieldSchema : fieldSchemas) {
            Assert.assertEquals(fieldSchema.getName(),"merch_date");
            Assert.assertEquals(fieldSchema.getType(),"string");
            Assert.assertEquals(fieldSchema.getComment(),"this is merch date");
            break;
        }
    }
}
