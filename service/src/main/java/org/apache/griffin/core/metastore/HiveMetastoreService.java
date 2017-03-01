package org.apache.griffin.core.metastore;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


@Service
public class HiveMetastoreService {

    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreService.class);

    @Value("${hive.metastore.uris}")
    private String uris;

    @Value("${hive.metastore.dbname}")
    private String defaultDbName;

    @Override
    public String toString() {
        return uris + " " + defaultDbName;
    }

    private HiveMetaStoreClient getHiveMetastoreClient() throws MetaException {
        return getHiveMetastoreClient(uris);
    }

    private HiveMetaStoreClient getHiveMetastoreClient(String url) throws MetaException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, url);
        return new HiveMetaStoreClient(hiveConf);
    }

    public Iterable<String> getAllDatabases() {
        Iterable<String> results = null;
        HiveMetaStoreClient client = null;
        try {
            client = getHiveMetastoreClient();
            results = client.getAllDatabases();
        } catch (MetaException e) {
            log.error("Can not get databases",e.getMessage());
        }finally {
            client.close();
        }
        return results;
    }

    public Iterable<String> getAllTables(String dbName) {
        Iterable<String> results = null;
        HiveMetaStoreClient client = null;
        String useDbName = dbName;
        if (!StringUtils.hasText(dbName)) useDbName = defaultDbName;
        try {
            client = getHiveMetastoreClient();
            results = client.getAllTables(useDbName);
        } catch (Exception e) {
            log.warn("Exception fetching tables info" + e.getMessage());
        }finally {
            client.close();
        }
        return results;
    }

    public Table getTable(String dbName, String tableName) {
        Table result = null;
        String useDbName = dbName;
        HiveMetaStoreClient client = null;
        if (!StringUtils.hasText(dbName)) useDbName = defaultDbName;
        try {
            client = getHiveMetastoreClient();
            result = client.getTable(useDbName, tableName);
        } catch (Exception e) {
            log.warn("Exception fetching table info : " +tableName + " : " + e.getMessage());
        }finally {
            client.close();

        }
        return result;
    }

    public Iterable<FieldSchema> getSchema(String dbName, String tableName) {
        Iterable<FieldSchema> results = null;
        String useDbName = dbName;
        HiveMetaStoreClient client = null;
        if (!StringUtils.hasText(useDbName)) useDbName = defaultDbName;
        try {
            client = getHiveMetastoreClient();
            results = client.getSchema(useDbName, tableName);
        } catch (Exception e) {
            log.warn("Exception fetching schema info : " +tableName + " : " + e.getMessage());
        }finally {
            client.close();

        }
        return results;
    }


    public Partition getPartition(String dbName, String tableName, PartitionItems partitionItems) {
        Partition result = null;
        String useDbName = dbName;
        HiveMetaStoreClient client = null;
        if (!StringUtils.hasText(useDbName)) useDbName = defaultDbName;
        try {
            client = getHiveMetastoreClient();
            result = client.getPartition(useDbName, tableName, partitionItems.toString());
        } catch (Exception e) {
            log.warn("Exception fetching partition info : " +tableName + " : " +partitionItems+" : "+ e.getMessage());
        }finally {
            client.close();

        }
        return result;
    }

    public Iterable<Partition> getPartitions(String dbName, String tableName, PartitionItemsList partitionItemsList) {
        Iterable<Partition> results = null;
        String useDbName = dbName;
        if (!StringUtils.hasText(useDbName)) useDbName = defaultDbName;
        HiveMetaStoreClient client = null;
        try {
            client = getHiveMetastoreClient();
            results = client.getPartitionsByNames(useDbName, tableName, partitionItemsList.generateString());
        } catch (Exception e) {
            log.warn("Exception fetching partition info : " +tableName + " : " +partitionItemsList+" : "+ e.getMessage());
        }finally {
            client.close();
        }
        return results;
    }
}
