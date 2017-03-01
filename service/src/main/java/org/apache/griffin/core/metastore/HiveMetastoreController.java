package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hive/metastore")
public class HiveMetastoreController {

    @Autowired
    HiveMetastoreService hiveMetastoreService;

    @RequestMapping("/version")
    public String greeting() {
        return "0.1.0";
    }

    @RequestMapping("/dbs")
    public Iterable<String> getAllDatabases() {
        return hiveMetastoreService.getAllDatabases();
    }

    @RequestMapping("/tables")
    public Iterable<String> getDefAllTables() {
        return hiveMetastoreService.getAllTables("");
    }

    @RequestMapping("/tables/{db}")
    public Iterable<String> getAllTables(@PathVariable("db") String dbName) {
        return hiveMetastoreService.getAllTables(dbName);
    }

    @RequestMapping("/table/{table}")
    public Table getDefTable(@PathVariable("table") String tableName) {
        return hiveMetastoreService.getTable("", tableName);
    }

    @RequestMapping("/table/{db}/{table}")
    public Table getTable(@PathVariable("db") String dbName, @PathVariable("table") String tableName) {
        return hiveMetastoreService.getTable(dbName, tableName);
    }

    @RequestMapping("/schema/{table}")
    public Iterable<FieldSchema> getDefSchema(@PathVariable("table") String tableName) {
        return hiveMetastoreService.getSchema("", tableName);
    }

    @RequestMapping("/schema/{db}/{table}")
    public Iterable<FieldSchema> getSchema(@PathVariable("db") String dbName, @PathVariable("table") String tableName) {
        return hiveMetastoreService.getSchema(dbName, tableName);
    }

    @RequestMapping(value="/partition/{table}", method= RequestMethod.POST)
    public Partition getDefPartition(@PathVariable("table") String tableName, @RequestBody PartitionItems partitionItems) {
        return hiveMetastoreService.getPartition("", tableName, partitionItems);
    }

    @RequestMapping(value="/partition/{db}/{table}", method= RequestMethod.POST)
    public Partition getPartition(@PathVariable("db") String dbName, @PathVariable("table") String tableName, @RequestBody PartitionItems partitionItems) {
        return hiveMetastoreService.getPartition(dbName, tableName, partitionItems);
    }

    @RequestMapping(value="/partitions/{table}", method= RequestMethod.POST)
    public Iterable<Partition> getDefPartitions(@PathVariable("table") String tableName, @RequestBody PartitionItemsList partitionItemsList) {
        return hiveMetastoreService.getPartitions("", tableName, partitionItemsList);
    }

    @RequestMapping(value="/partitions/{db}/{table}", method= RequestMethod.POST)
    public Iterable<Partition> getPartitions(@PathVariable("db") String dbName, @PathVariable("table") String tableName, @RequestBody PartitionItemsList partitionItemsList) {
        return hiveMetastoreService.getPartitions(dbName, tableName, partitionItemsList);
    }
}
