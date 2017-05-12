package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.util.AssertionErrors.assertEquals;

/**
 * Created by xiangrchen on 5/10/17.
 */
public class HiveMetastoreServiceTest {

    HiveMetastoreService hiveMetastoreService;
    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        Field defaultDbName = HiveMetastoreService.class.getDeclaredField("defaultDbName");
        defaultDbName.setAccessible(true);
        hiveMetastoreService=new HiveMetastoreService();
        defaultDbName.set(hiveMetastoreService,"default");
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
}
