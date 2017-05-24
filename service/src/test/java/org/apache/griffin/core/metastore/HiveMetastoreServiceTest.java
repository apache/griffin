package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.util.AssertionErrors.assertEquals;


@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveMetastoreService.class})
public class HiveMetastoreServiceTest {

    @InjectMocks
    private HiveMetastoreService hiveMetastoreService;

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
//        Field defaultDbName = HiveMetastoreService.class.getDeclaredField("defaultDbName");
//        defaultDbName.setAccessible(true);
//        hiveMetastoreService=new HiveMetastoreService();
//        defaultDbName.set(hiveMetastoreService,"default");
        Whitebox.setInternalState(hiveMetastoreService,"defaultDbName","default");
        hiveMetastoreService.client=mock(HiveMetaStoreClient.class);
    }

    @Test
    public void test_getUseDbName() throws Exception {
        String dbName="someDbName";
        String result = Whitebox.invokeMethod(hiveMetastoreService, "getUseDbName", dbName);
        assertEquals("success",result,dbName);

        dbName="";
        result = Whitebox.invokeMethod(hiveMetastoreService, "getUseDbName", dbName);
        assertEquals("success",result,"default");
    }

    @Test
    public void test_getAllDatabases() throws MetaException {
        List<String> res= new ArrayList<>();
        when(hiveMetastoreService.client.getAllDatabases()).thenReturn(res);
        hiveMetastoreService.getAllDatabases();
        verify(hiveMetastoreService.client).getAllDatabases();

        when(hiveMetastoreService.client.getAllDatabases()).thenThrow(new MetaException());
        hiveMetastoreService.getAllDatabases();
    }

    @Test
    public void test_getAllTableNames() throws Exception{
        String dbName="dfs";
        List<String> res= new ArrayList<>();
        when(hiveMetastoreService.client.getAllTables(dbName)).thenReturn(res);
        hiveMetastoreService.getAllTableNames(dbName);
        verify(hiveMetastoreService.client).getAllTables(dbName);

        when(hiveMetastoreService.client.getAllTables(dbName)).thenThrow(new MetaException());
        hiveMetastoreService.getAllTableNames(dbName);
    }

    @Test
    public void test_getAllTable() throws Exception{
        String db="dff";
        List<String> tables= new ArrayList<>(Arrays.asList("cout","cout1"));
        when(hiveMetastoreService.client.getAllTables(db)).thenReturn(tables);
        for (String table:tables){
            when(hiveMetastoreService.client.getTable(db,table)).thenReturn(new Table());
        }
        hiveMetastoreService.getAllTable(db);
        verify(hiveMetastoreService.client).getAllTables(db);
        verify(hiveMetastoreService.client).getTable(db,tables.get(0));

        when(hiveMetastoreService.client.getAllTables(db)).thenThrow(new MetaException());
        hiveMetastoreService.getAllTable(db);
    }

    @Test
    public void test_getAllTable2() throws TException {
        List<String> dbs=new ArrayList<>(Arrays.asList("dff","dgg"));
        List<String> tables= new ArrayList<>(Arrays.asList("cout","cout1"));
        when(hiveMetastoreService.getAllDatabases()).thenReturn(dbs);
        for (String db:dbs){
            when(hiveMetastoreService.client.getAllTables(db)).thenReturn(tables);
            for (String table:tables){
                when(hiveMetastoreService.client.getTable(db,table)).thenReturn(new Table());
            }
        }
        hiveMetastoreService.getAllTable();
        verify(hiveMetastoreService.client).getAllTables(dbs.get(0));
        verify(hiveMetastoreService.client).getTable(dbs.get(0),tables.get(0));

        when(hiveMetastoreService.client.getAllTables(dbs.get(0))).thenThrow(new MetaException());
        hiveMetastoreService.getAllTable();
    }

    @Test
    public void test_getTable() throws Exception{
        String dbName="aaa";
        String tableName="ccc";
        when(hiveMetastoreService.client.getTable(dbName,tableName)).thenReturn(new Table());
        hiveMetastoreService.getTable(dbName,tableName);
        verify(hiveMetastoreService.client).getTable(dbName,tableName);

        when(hiveMetastoreService.client.getTable(dbName,tableName)).thenThrow(new MetaException());
        hiveMetastoreService.getTable(dbName,tableName);
    }
}
