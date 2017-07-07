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
package org.apache.griffin.core.metastore;


import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
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
    HiveMetastoreServiceImpl hiveMetastoreService;

    @RequestMapping("/db")
    public Iterable<String> getAllDatabases() throws MetaException{
        return hiveMetastoreService.getAllDatabases();
    }

    @RequestMapping("/table")
    public Iterable<String> getDefAllTables() throws MetaException{
        return hiveMetastoreService.getAllTableNames("");
    }

    @RequestMapping("/{db}/table")
    public Iterable<String> getAllTableNamess(@PathVariable("db") String dbName) throws MetaException{
        return hiveMetastoreService.getAllTableNames(dbName);
    }

    @RequestMapping("/{db}/alltables")
    public List<Table> getAllTables(@PathVariable("db") String dbName) throws TException {
        return hiveMetastoreService.getAllTable(dbName);
    }

    @RequestMapping("/alltables")
    public Map<String,List<Table>> getAllTables() throws TException{
        return hiveMetastoreService.getAllTable();
    }

    @RequestMapping("/table/{table}")
    public Table getDefTable(@PathVariable("table") String tableName) throws TException{
        return hiveMetastoreService.getTable("", tableName);
    }

    @RequestMapping("/{db}/table/{table}")
    public Table getTable(@PathVariable("db") String dbName, @PathVariable("table") String tableName) throws TException{
        return hiveMetastoreService.getTable(dbName, tableName);
    }
}
