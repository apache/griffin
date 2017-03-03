package org.apache.griffin.core.metastore;


import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


@Service
public class HiveMetastoreService {

    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreService.class);

    @Autowired
    HiveMetaStoreClient client;

    @Value("${hive.metastore.dbname}")
    private String defaultDbName;

    private String getUseDbName(String dbName) {
        if (!StringUtils.hasText(dbName)) return defaultDbName;
        else return dbName;
    }

    public Iterable<String> getAllDatabases() {
        Iterable<String> results = null;
        try {
            results = client.getAllDatabases();
        } catch (MetaException e) {
            log.error("Can not get databases : ",e.getMessage());
        }
        return results;
    }

    public Iterable<String> getAllTables(String dbName) {
        Iterable<String> results = null;
        String useDbName = getUseDbName(dbName);
        try {
            results = client.getAllTables(useDbName);
        } catch (Exception e) {
            log.warn("Exception fetching tables info" + e.getMessage());
        }
        return results;
    }

    public Table getTable(String dbName, String tableName) {
        Table result = null;
        String useDbName = getUseDbName(dbName);
        try {
            result = client.getTable(useDbName, tableName);
        } catch (Exception e) {
            log.warn("Exception fetching table info : " +tableName + " : " + e.getMessage());
        }
        return result;
    }


}
