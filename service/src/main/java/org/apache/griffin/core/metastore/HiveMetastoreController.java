package org.apache.griffin.core.metastore;


import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/metadata/hive")
public class HiveMetastoreController {

    @Autowired
    HiveMetastoreService hiveMetastoreService;

    @RequestMapping("/db")
    public Iterable<String> getAllDatabases() {
        return hiveMetastoreService.getAllDatabases();
    }

    @RequestMapping("/table")
    public Iterable<String> getDefAllTables() {
        return hiveMetastoreService.getAllTableNames("");
    }

    @RequestMapping("/{db}/table")
    public Iterable<String> getAllTableNamess(@PathVariable("db") String dbName) {
        return hiveMetastoreService.getAllTableNames(dbName);
    }

    @RequestMapping("/{db}/alltables")
    public List<Table> getAllTables(@PathVariable("db") String dbName) {
        return hiveMetastoreService.getAllTable(dbName);
    }

    @RequestMapping("/alltables")
    public Map<String,List<Table>> getAllTables() {
        return hiveMetastoreService.getAllTable();
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
