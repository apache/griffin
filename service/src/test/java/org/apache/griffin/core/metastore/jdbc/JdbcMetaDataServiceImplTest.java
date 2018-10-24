package org.apache.griffin.core.metastore.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.griffin.core.config.CacheConfig;
import org.apache.griffin.core.config.DatasourceConfig;
import org.apache.griffin.core.metastore.jdbc.model.TableMetaData;
import org.apache.griffin.core.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.fasterxml.jackson.core.JsonProcessingException;

@RunWith(SpringRunner.class)
@ContextConfiguration(
        classes = {DatasourceConfig.class, JdbcMetaDataServiceImplTest.JdbcMetadataServiceConfiguration.class},
        loader = AnnotationConfigContextLoader.class)
@TestPropertySource(locations = "classpath:application.properties",
        properties = "external.config.location=src/main/resources")
public class JdbcMetaDataServiceImplTest {

    @TestConfiguration
    @EnableCaching
    // @ContextConfiguration(classes = DatasourceConfig.class, loader =
    // AnnotationConfigContextLoader.class)
    public static class JdbcMetadataServiceConfiguration extends CacheConfig {

        @Bean("jdbcMetaDataServiceImpl")
        public JdbcMetaDataService service() {
            return new JdbcMetaDataServiceImpl();
        }

        @Bean
        CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("jdbc");
        }
    }


    @Autowired
    private JdbcMetaDataService service;

    @Autowired
    private CacheManager cacheManager;

    @Before
    public void setup() {
        cacheManager.getCache("jdbc").clear();
    }

    @Test
    public void testGetDatabases() {
        System.out.println("jdbcMetadataService.getDatabases(\"sqlserver\") = " + service.getDatabases("sqlserver"));
    }

    @Test
    public void testGetSchemas() {
        List<String> schemas = service.getSchemas("sqlserver", "griffin");
        System.out.println("schemas = " + schemas);
        assertTrue(!schemas.isEmpty() && schemas.contains("dbo"));
    }

    @Test
    public void testGetTables() {
        List<TableMetaData> tables = service.getTables("sqlserver", "griffin");
        try {
            System.out.println("tables = " + JsonUtil.toJson(tables));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        assertTrue(tables != null && !tables.isEmpty());
    }

    @Test
    public void testGetTable() {
        TableMetaData table = service.getTable("sqlserver", "griffin", "dbo", "demo_src");
        try {
            System.out.println("JsonUtil.toJsonWithFormat(table) = " + JsonUtil.toJsonWithFormat(table));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        assertNotNull(table);
        assertEquals("demo_src", table.getTableName());
        assertTrue(!table.getColumns().isEmpty());
    }
}
