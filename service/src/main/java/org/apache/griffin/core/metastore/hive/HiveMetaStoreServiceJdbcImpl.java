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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;





@Service
@Qualifier(value = "hive_jdbc")
@CacheConfig(cacheNames = "jdbchive", keyGenerator = "cacheKeyGenerator")
public class HiveMetaStoreServiceJdbcImpl implements HiveMetaStoreService {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(HiveMetaStoreService.class);

    @Value("${hive.jdbc.className}")
    private String hiveClassName;

    @Value("${hive.jdbc.url}")
    private String hiveUrl;

    private Connection conn;

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public void setHiveClassName(String hiveClassName) {
        this.hiveClassName = hiveClassName;
    }

    @Override
    @Cacheable(unless = "#result==null")
    public Iterable<String> getAllDatabases() {
        String sql = "show databases";
        return queryHiveString(sql);
    }

    @Override
    @Cacheable(unless = "#result==null")
    public Iterable<String> getAllTableNames(String dbName) {
        String sql = "show tables in " + dbName;
        return queryHiveString(sql);
    }

    @Override
    @Cacheable(unless = "#result==null")
    public Map<String, List<String>> getAllTableNames() {
        Map<String, List<String>> res = new HashMap<>();

        for (String dbName : getAllDatabases()) {
            List<String> list = (List<String>) queryHiveString("show tables in " + dbName);
            res.put(dbName, list);
        }
        return res;
    }

    @Override
    public List<Table> getAllTable(String db) {
        return null;
    }

    @Override
    public Map<String, List<Table>> getAllTable() {
        return null;
    }

    @Override
    @Cacheable(unless = "#result==null")
    public Table getTable(String dbName, String tableName) {
        Table result = new Table();
        result.setDbName(dbName);
        result.setTableName(tableName);

        String sql = "show create table " + dbName + "." + tableName;
        Statement stmt = null;
        ResultSet rs = null;
        StringBuilder sb = new StringBuilder();

        try {
            Class.forName(hiveClassName);
            if (conn == null) {
                conn = DriverManager.getConnection(hiveUrl);
            }
            LOGGER.info("got connection");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String s = rs.getString(1);
                sb.append(s);
            }
            String location = getLocation(sb.toString());
            List<FieldSchema> cols = getColums(sb.toString());
            StorageDescriptor sd = new StorageDescriptor();
            sd.setLocation(location);
            sd.setCols(cols);
            result.setSd(sd);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public void evictHiveCache() {
        LOGGER.info("Evict hive cache");
    }

    /**
     * Query Hive for Show tables or show databases, which will return List of String
     * @param sql sql string
     * @return
     */
    private Iterable<String> queryHiveString(String sql) {
        List<String> res = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;

        try {
            Class.forName(hiveClassName);
            if (conn == null) {
                conn = DriverManager.getConnection(hiveUrl);
            }
            LOGGER.info("got connection");
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                res.add(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * Get the Hive table location from hive table metadata string
     * @param tableMetadata hive table metadata string
     * @return Hive table location
     */
    public String getLocation(String tableMetadata) {
        tableMetadata = tableMetadata.toLowerCase();
        int index = tableMetadata.indexOf("location");
        if (index == -1) {
            return "";
        }

        int start = tableMetadata.indexOf("\'", index);
        int end = tableMetadata.indexOf("\'", start + 1);

        if (start == -1 || end == -1) {
            return "";
        }

        return tableMetadata.substring(start + 1, end);
    }

    /**
     * Get the Hive table schema: column name, column type, column comment
     * @param tableMetadata hive table metadata string
     * @return List of FieldSchema
     */
    public List<FieldSchema> getColums(String tableMetadata) {
        List<FieldSchema> res = new ArrayList<>();
        int start = tableMetadata.indexOf("(") + 1;
        int end = tableMetadata.indexOf(")", start);
        String[] colsArr = tableMetadata.substring(start, end).split(",");
        for (String colStr : colsArr) {
            colStr = colStr.trim();
            String[] parts = colStr.split(" ");
            String colName = parts[0].trim().substring(1, parts[0].trim().length() - 1);
            String colType = parts[1].trim();
            String comment = getComment(colStr);
            FieldSchema schema = new FieldSchema(colName, colType, comment);
            res.add(schema);
        }
        return res;
    }

    /**
     * Parse one column string, such as : `merch_date` string COMMENT 'this is merch process date'
     *
     * @param colStr column string
     * @return
     */
    public String getComment(String colStr) {
        colStr = colStr.toLowerCase();
        int i = colStr.indexOf("comment");
        if (i == -1) {
            return "";
        }

        int s = -1;
        int e = -1;
        while (i < colStr.length()) {
            if (colStr.charAt(i) == '\'') {
                if (s == -1) {
                    s = i;
                } else {
                    e = i;
                    break;
                }
            }
            i++;
        }
        if (s == -1 || e == -1) {
            return "";
        }
        if (s > e) {
            return "";
        }

        return colStr.substring(s + 1, e);
    }
}
