package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/metastore/hive")
public class HiveMetastoreController {

    @Autowired
    HiveMetastoreService hiveMetastoreService;

    @RequestMapping("/db")
    public Iterable<String> getAllDatabases() {
        return hiveMetastoreService.getAllDatabases();
    }

    @RequestMapping("/table")
    public Iterable<String> getDefAllTables() {
        return hiveMetastoreService.getAllTables("");
    }

    @RequestMapping("/{db}/table")
    public Iterable<String> getAllTables(@PathVariable("db") String dbName) {
        return hiveMetastoreService.getAllTables(dbName);
    }

    @RequestMapping("/table/{table}")
    public Table getDefTable(@PathVariable("table") String tableName) {
        return hiveMetastoreService.getTable("", tableName);
    }

    @RequestMapping("/{db}/table/{table}")
    public Table getTable(@PathVariable("db") String dbName, @PathVariable("table") String tableName) {
        return hiveMetastoreService.getTable(dbName, tableName);
    }


}
