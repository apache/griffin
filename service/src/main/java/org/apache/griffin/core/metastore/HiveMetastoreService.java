package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.List;

@PropertySource("classpath:hive-conf.properties")
@Service
public class HiveMetastoreService {

    @Value("${hive.conf.metastore.uris}")
    private String uris;

    @Value("${hive.default.dbname}")
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
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            results = client.getAllDatabases();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    public Iterable<String> getAllTables(String dbName) {
        Iterable<String> results = null;
        String useDbName = dbName;
        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            results = client.getAllTables(useDbName);
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    public Table getTable(String dbName, String tableName) {
        Table result = null;
        String useDbName = dbName;
        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            result = client.getTable(useDbName, tableName);
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Iterable<FieldSchema> getSchema(String dbName, String tableName) {
        Iterable<FieldSchema> results = null;
        String useDbName = dbName;
        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            results = client.getSchema(useDbName, tableName);
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

//    public Partition getPartition(String dbName, String tableName, String partName) {
//        Partition result = null;
//        String useDbName = dbName;
//        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
//        try {
//            HiveMetaStoreClient client = getHiveMetastoreClient();
//            result = client.getPartition(useDbName, tableName, partName);
//            client.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return result;
//    }

    public Partition getPartition(String dbName, String tableName, PartitionItems partitionItems) {
        Partition result = null;
        String useDbName = dbName;
        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            result = client.getPartition(useDbName, tableName, partitionItems.generateString());
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public Iterable<Partition> getPartitions(String dbName, String tableName, PartitionItemsList partitionItemsList) {
        Iterable<Partition> results = null;
        String useDbName = dbName;
        if (useDbName == null || useDbName.equals("")) useDbName = defaultDbName;
        try {
            HiveMetaStoreClient client = getHiveMetastoreClient();
            results = client.getPartitionsByNames(useDbName, tableName, partitionItemsList.generateString());
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}
