package org.apache.griffin.core.metastore;


import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

    public Iterable<String> getAllTableNames(String dbName) {
        Iterable<String> results = null;
        String useDbName = getUseDbName(dbName);
        try {
            results = client.getAllTables(useDbName);
        } catch (Exception e) {
            log.warn("Exception fetching tables info" + e.getMessage());
        }
        return results;
    }

    public List<Table> getAllTable(String db){
        List<Table> results = new ArrayList<Table>();
        String useDbName = getUseDbName(db);
        try {
            Iterable<String> tables = client.getAllTables(useDbName);
            for (String table: tables) {
                Table tmp = client.getTable(db,table);
                results.add(tmp);
            }
        } catch (Exception e) {
            log.warn("Exception fetching tables info" + e.getMessage());
        }
        return results;
    }

    public Map<String,List<Table>> getAllTable(){
        Map<String,List<Table>> results = new HashMap<String, List<Table>>();
        Iterable<String> dbs = getAllDatabases();
        for(String db: dbs){
            List<Table> alltables = new ArrayList<Table>();
            String useDbName = getUseDbName(db);
            try {
                Iterable<String> tables = client.getAllTables(useDbName);
                for (String table: tables) {
                    Table tmp = client.getTable(db,table);
                    alltables.add(tmp);
                }
            } catch (Exception e) {
                log.warn("Exception fetching tables info" + e.getMessage());
            }
            results.put(db,alltables);
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
