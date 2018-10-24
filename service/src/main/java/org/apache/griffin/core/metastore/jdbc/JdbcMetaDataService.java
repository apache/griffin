package org.apache.griffin.core.metastore.jdbc;

import java.util.List;

import org.apache.griffin.core.metastore.jdbc.model.DatabaseMetaData;
import org.apache.griffin.core.metastore.jdbc.model.TableMetaData;
import org.springframework.cache.annotation.Cacheable;

public interface JdbcMetaDataService {
    List<DatabaseMetaData> getDatabases(String dbType);

    List<String> getSchemas(String dbType, String database);

    List<TableMetaData> getTables(String dbType, String database);

    TableMetaData getTable(String dbType, String database, String schema, String tableName);

    void evictHiveCache();
}
