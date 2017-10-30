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


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Api(tags = "Hive metastore",description = "hive table and database manipulation")
@RestController
@RequestMapping("/api/v1/metadata/hive")
public class HiveMetaStoreController {

    @Autowired
    private HiveMetaStoreService hiveMetaStoreService;

    @ApiOperation(value = "Get database names", response = Iterable.class)
    @RequestMapping(value = "/dbs", method = RequestMethod.GET)
    public Iterable<String> getAllDatabases() {
        return hiveMetaStoreService.getAllDatabases();
    }


    @ApiOperation(value = "Get table names", response = Iterable.class)
    @RequestMapping(value = "/tables/names", method = RequestMethod.GET)
    public Iterable<String> getAllTableNames(@ApiParam(value = "hive db name", required = true) @RequestParam("db") String dbName) {
        return hiveMetaStoreService.getAllTableNames(dbName);
    }

    @ApiOperation(value = "Get tables metadata", response = List.class)
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public List<Table> getAllTables(@ApiParam(value = "hive db name", required = true) @RequestParam("db") String dbName) {
        return hiveMetaStoreService.getAllTable(dbName);
    }

    @ApiOperation(value = "Get all database tables metadata", response = Map.class)
    @RequestMapping(value = "/dbs/tables", method = RequestMethod.GET)
    public Map<String, List<Table>> getAllTables() {
        return hiveMetaStoreService.getAllTable();
    }


    @ApiOperation(value = "Get table metadata", response = Table.class)
    @RequestMapping(value = "/table", method = RequestMethod.GET)
    public Table getTable(@ApiParam(value = "hive database name", required = true) @RequestParam("db") String dbName,
                          @ApiParam(value = "hive table name", required = true) @RequestParam("table") String tableName) {
        return hiveMetaStoreService.getTable(dbName, tableName);
    }


}
