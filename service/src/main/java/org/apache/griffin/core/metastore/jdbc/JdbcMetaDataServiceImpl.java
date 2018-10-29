package org.apache.griffin.core.metastore.jdbc;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.springframework.jdbc.support.JdbcUtils.closeConnection;
import static org.springframework.jdbc.support.JdbcUtils.extractDatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.griffin.core.metastore.jdbc.model.ColumnMetaData;
import org.apache.griffin.core.metastore.jdbc.model.DatabaseMetaData;
import org.apache.griffin.core.metastore.jdbc.model.TableMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Service
@CacheConfig(cacheNames = "jdbc", keyGenerator = "cacheKeyGenerator")
public class JdbcMetaDataServiceImpl implements JdbcMetaDataService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetaDataServiceImpl.class);
    @Value("#{environment.getProperty('ds', T(java.util.Map))}")
    private Map<String, Map<String, Map<String, String>>> datasources;

    public JdbcMetaDataServiceImpl() {}

    private DataSource createDataSource(String dbType, String database) {
        Map<String, String> ds = datasources.getOrDefault(dbType, emptyMap()).getOrDefault(database, emptyMap());
        if (ds.isEmpty()) {
            return null;
        }
        return DataSourceBuilder.create().url(ds.get("url")).username(ds.get("username")).password(ds.get("password"))
                .build();
    }

    private <T> Set<T> valuesFromResultSet(ResultSet rs, Class<T> resultClass, boolean firstOnly) {
        Set<T> values = Sets.newHashSet();
        if (resultClass == TableMetaData.class) {
            return valuesFromResultSet(rs, Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "REMARKS"), cm -> {
                TableMetaData tmd = new TableMetaData();
                tmd.setDatabase(cm.get("TABLE_CAT"));
                tmd.setSchema(cm.get("TABLE_SCHEM"));
                tmd.setTableName(cm.get("TABLE_NAME"));
                tmd.setFullTableName(
                        Joiner.on(".").skipNulls().join(defaultIfEmpty(tmd.getSchema(), null), tmd.getTableName()));
                tmd.setComments(cm.get("REMARKS"));
                return resultClass.cast(tmd);
            }, firstOnly);
        } else if (resultClass == ColumnMetaData.class) {
            return valuesFromResultSet(rs, Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                    "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "IS_NULLABLE", "REMARKS", "COLUMN_DEF"), cm -> {
                        ColumnMetaData cmd = new ColumnMetaData();
                        cmd.setDatabase(cm.get("TABLE_CAT"));
                        cmd.setSchema(cm.get("TABLE_SCHEM"));
                        cmd.setTableName(cm.get("TABLE_NAME"));
                        cmd.setColumnName(cm.get("COLUMN_NAME"));
                        cmd.setColumnType(cm.get("TYPE_NAME"));
                        cmd.setColumnSize(toInt(cm.get("COLUMN_SIZE"), 0));
                        cmd.setDecimalDigits(toInt(cm.get("DECIMAL_DIGITS"), 0));
                        cmd.setNullable(cm.get("IS_NULLABLE"));
                        cmd.setComments(cm.get("REMARKS"));
                        cmd.setDefaultValue(cm.get("COLUMN_DEF"));
                        return resultClass.cast(cmd);
                    }, firstOnly);
        }
        return values;
    }

    private <T> Set<T> valuesFromResultSet(ResultSet rs, List<String> columns,
            Function<Map<String, String>, T> valueFunction, boolean firstOnly) {
        Set<T> values = Sets.newHashSet();
        try {
            while (rs.next()) {
                Map<String, String> colMap = new HashMap<>();
                for (String k : columns) {
                    colMap.put(k, rs.getString(k));
                }
                values.add(valueFunction.apply(colMap));
                if (firstOnly) {
                    break;
                }
            }
            JdbcUtils.closeResultSet(rs);
        } catch (SQLException e) {
            LOGGER.error("Error getting column values", e);
        }
        return values;
    }

    private void cleanUp(DataSource dataSource) {
        if (dataSource != null) {
            LOGGER.debug("Cleanup datasource connection");
            try {
                closeConnection(dataSource.getConnection());
            } catch (SQLException e) {
                LOGGER.error("Error while closing connection", e);
            }
        }
    }

    @Override
    @Cacheable(unless = "#result==null || #result.isEmpty()")
    public List<DatabaseMetaData> getDatabases(String dbType) {
        return datasources.getOrDefault(dbType, emptyMap()).entrySet().stream().map(entry -> {
            DatabaseMetaData dmd = new DatabaseMetaData();
            dmd.setUrl(String.join(":", entry.getValue().get("host"), entry.getValue().get("port")));
            dmd.setDatabase(entry.getValue().get("database"));
            dmd.setUsername(entry.getValue().get("username"));
            dmd.setPassword(entry.getValue().get("password"));
            return dmd;
        }).collect(toList());
    }

    @Override
    @Cacheable(unless = "#result==null || #result.isEmpty()")
    public List<String> getSchemas(String dbType, String database) {
        List<String> schemas = Lists.newArrayList();

        DataSource dataSource = createDataSource(dbType, database);

        if (dataSource == null) {
            return schemas;
        }

        try {
            extractDatabaseMetaData(dataSource, dbmd -> schemas.addAll(valuesFromResultSet(dbmd.getSchemas(),
                    singletonList("TABLE_SCHEM"), m -> m.get("TABLE_SCHEM"), false)));
        } catch (MetaDataAccessException e) {
            LOGGER.error("Error while getting schema list for {}/{}", dbType, database);
        } finally {
            cleanUp(dataSource);
        }

        return schemas;
    }

    @Override
    @Cacheable(unless = "#result==null || #result.isEmpty()")
    public List<TableMetaData> getTables(String dbType, String database) {
        List<TableMetaData> tables = Lists.newArrayList();
        DataSource dataSource = createDataSource(dbType, database);

        if (dataSource == null) {
            return tables;
        }

        try {
            extractDatabaseMetaData(dataSource, dbmd -> {
                Iterable<String> schemas = Splitter.on(',').omitEmptyStrings().trimResults()
                        .split(datasources.getOrDefault(dbType, emptyMap()).getOrDefault(database, emptyMap())
                                .getOrDefault("schemas", ""));
                for (String schema : schemas) {
                    ResultSet resultSet = dbmd.getTables(null, schema, null, new String[] {"VIEW", "TABLE"});
                    tables.addAll(valuesFromResultSet(resultSet, TableMetaData.class, false));
                }
                for (TableMetaData tmd : tables) {
                    tmd.setColumns(new ArrayList<>(
                            valuesFromResultSet(dbmd.getColumns(database, tmd.getSchema(), tmd.getTableName(), null),
                                    ColumnMetaData.class, false)));
                }
                return tables;
            });
        } catch (MetaDataAccessException e) {
            LOGGER.error("Error while getting table list for {}/{}", dbType, database);
        } finally {
            cleanUp(dataSource);
        }
        return tables;
    }

    @Override
    @Cacheable(unless = "#result==null")
    public TableMetaData getTable(String dbType, String database, String schema, String tableName) {
        TableMetaData table = null;
        DataSource dataSource = createDataSource(dbType, database);

        if (dataSource == null) {
            return null;
        }

        try {
            table = (TableMetaData) extractDatabaseMetaData(dataSource, dbmd -> {
                Set<TableMetaData> tables = valuesFromResultSet(dbmd.getTables(database, schema, tableName, null),
                        TableMetaData.class, true);
                if (tables.isEmpty()) {
                    return null;
                }
                Optional<TableMetaData> first = tables.stream().findFirst();

                ResultSet columns = dbmd.getColumns(database, schema, tableName, null);
                first.ifPresent(tmd -> tmd
                        .setColumns(new ArrayList<>(valuesFromResultSet(columns, ColumnMetaData.class, false))));
                return first.orElse(null);
            });
        } catch (MetaDataAccessException e) {
            LOGGER.error("Error while getting table metadata for {}/{}/{}", dbType, database, tableName);
        } finally {
            cleanUp(dataSource);
        }
        return table;
    }

    @Override
    @Scheduled(fixedRateString = "${cache.evict.jdbc.fixedRate.in.milliseconds:1800000}")
    @CacheEvict(cacheNames = "true", allEntries = true, beforeInvocation = true)
    public void evictHiveCache() {
        LOGGER.info("Evict jdbc cache");
    }
}
