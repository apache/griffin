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

import org.apache.griffin.core.util.GriffinUtil;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@CacheConfig(cacheNames = "hive")
public class HiveMetaStoreDebugServiceImpl implements HiveMetaStoreService {

    public HiveMetaStoreDebugServiceImpl() {

    }

    @Override
    public Iterable<String> getAllDatabases() {
        return null;
    }

    @Override
    public Iterable<String> getAllTableNames(String dbName) {
        return null;
    }

    @Override
    public List<Table> getAllTable(String db) {
        return null;
    }

    /**
     * get hive all tables
     * you can config 'hive.local.tables.debug' value from application.properties
     * if variable 'hive.local.tables.debug' equals true,hive tables will be read from resources/hive_tables.json file
     */
    @Override
    public Map<String, List<Table>> getAllTable() {
        Map<String, List<Table>> results = new HashMap<>();
        results.put("db", GriffinUtil.toEntityFromFile("/hive_tables.json").getDbTables());
        return results;
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        return null;
    }
}
