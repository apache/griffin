package org.apache.griffin.core.metastore.jdbc;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.List;

import org.apache.griffin.core.metastore.jdbc.model.DatabaseMetaData;
import org.apache.griffin.core.metastore.jdbc.model.TableMetaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1/metadata/jdbc", produces = APPLICATION_JSON)
public class JdbcMetaDataController {

    @Autowired
    private JdbcMetaDataService jdbcMetaDataService;

    @RequestMapping(path = "{dbType}/dbs")
    public List<DatabaseMetaData> getAllDatabases(@PathVariable String dbType) {
        return jdbcMetaDataService.getDatabases(dbType);
    }

    @RequestMapping(path = "{dbType}/dbs/{db}/schemas")
    public List<String> getSchemas(@PathVariable String dbType, @PathVariable String db) {
        return jdbcMetaDataService.getSchemas(dbType, db);
    }

    @RequestMapping(path = "{dbType}/dbs/{db}/tables")
    public List<TableMetaData> getTables(@PathVariable String dbType, @PathVariable String db) {
        return jdbcMetaDataService.getTables(dbType, db);
    }

    @RequestMapping(path = "{dbType}/dbs/{db}/tables/{schema}/{table}")
    public TableMetaData getTable(@PathVariable String dbType, @PathVariable String db, @PathVariable String schema,
            @PathVariable String table) {
        return jdbcMetaDataService.getTable(dbType, db, schema, table);
    }
}
