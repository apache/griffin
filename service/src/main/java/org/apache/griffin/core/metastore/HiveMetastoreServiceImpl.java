/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class HiveMetastoreServiceImpl implements HiveMetastoreService{

    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreServiceImpl.class);

    @Autowired
    HiveMetaStoreClient client;

    @Value("${hive.metastore.dbname}")
    private String defaultDbName;

    private String getUseDbName(String dbName) {
        if (!StringUtils.hasText(dbName)) return defaultDbName;
        else return dbName;
    }

    @Retryable(value = { MetaException.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    @Override
    public Iterable<String> getAllDatabases() throws MetaException{
        Iterable<String> results = null;
        try {
            results = client.getAllDatabases();
        } catch (MetaException e) {
            reconnect();
            log.error("Can not get databases : ",e.getMessage());
            throw e;
        }
        return results;
    }

    @Retryable(value = { MetaException.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    @Override
    public Iterable<String> getAllTableNames(String dbName) throws MetaException{
        Iterable<String> results = null;
        String useDbName = getUseDbName(dbName);
        try {
            results = client.getAllTables(useDbName);
        } catch (Exception e) {
            reconnect();
            log.warn("Exception fetching tables info" + e.getMessage());
            throw e;
        }
        return results;
    }

    @Retryable(value = { TException.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    @Override
    public List<Table> getAllTable(String db) throws TException{
        List<Table> results = new ArrayList<Table>();
        String useDbName = getUseDbName(db);
        try {
            Iterable<String> tables = client.getAllTables(useDbName);
            for (String table: tables) {
                Table tmp = client.getTable(db,table);
                results.add(tmp);
            }
        } catch (Exception e) {
            reconnect();
            log.warn("Exception fetching tables info" + e.getMessage());
            throw e;
        }
        return results;
    }

    @Retryable(value = { TException.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    @Override
    public Map<String,List<Table>> getAllTable() throws TException{
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
                reconnect();
                log.warn("Exception fetching tables info" + e.getMessage());
                throw e;
            }
            results.put(db,alltables);
        }
        return results;
    }

    @Retryable(value = { TException.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    @Override
    public Table getTable(String dbName, String tableName) throws TException{
        Table result = null;
        String useDbName = getUseDbName(dbName);
        try {
            result = client.getTable(useDbName, tableName);
        } catch (Exception e) {
            reconnect();
            log.warn("Exception fetching table info : " +tableName + " : " + e.getMessage());
            throw e;
        }
        return result;
    }

    private void reconnect() throws MetaException{
        client.reconnect();
    }
}
