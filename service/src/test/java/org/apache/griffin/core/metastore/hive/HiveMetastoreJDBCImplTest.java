package org.apache.griffin.core.metastore.hive;


import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Author: qwang8
 * Date:   2019-06-25.
 */
@RunWith(MockitoJUnitRunner.class)
public class HiveMetastoreJDBCImplTest {

    private HiveMetaStoreServiceJDBCImpl serviceJDBC = new HiveMetaStoreServiceJDBCImpl();

    private HiveConnectMgr hiveConnectMgr = HiveConnectMgr.getHiveConnectionMgr();

    @Mock
    private Connection conn;

    @Mock
    private Statement stmt;

    @Mock
    private ResultSet rs;

    @Before
    public void setUp() throws SQLException {

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
        hiveConnectMgr.setConn(conn);

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
        hiveConnectMgr.setConn(conn);

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
        hiveConnectMgr.setConn(conn);

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
