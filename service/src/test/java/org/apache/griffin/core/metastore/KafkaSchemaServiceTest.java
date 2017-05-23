package org.apache.griffin.core.metastore;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.springframework.test.util.AssertionErrors.assertEquals;

/**
 * Created by xiangrchen on 5/9/17.
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration
@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveMetastoreService.class})
public class KafkaSchemaServiceTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchemaServiceTest.class);

    @InjectMocks
    private KafkaSchemaService kafkaSchemaService;

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
//        Field url = KafkaSchemaService.class.getDeclaredField("url");
//        url.setAccessible(true);
//        kafkaSchemaService=new KafkaSchemaService();
//        url.set(kafkaSchemaService, "http://localhost:8080");
        Whitebox.setInternalState(kafkaSchemaService,"url","http://localhost:8080");
//        kafkaSchemaService.client=mock(HiveMetaStoreClient.class);
    }

    @Test
    public void test_registryUrl() throws Exception {
        String path="/user/id";
        String result = Whitebox.invokeMethod(kafkaSchemaService, "registryUrl", path);
        assertEquals("success",result,"http://localhost:8080"+path);

        path="user/id";
        result = Whitebox.invokeMethod(kafkaSchemaService, "registryUrl", path);
        assertEquals("success",result,"http://localhost:8080"+"/"+path);

        path="";
        result = Whitebox.invokeMethod(kafkaSchemaService, "registryUrl", path);
        assertEquals("success",result,path);
    }
    @Test
    public void test_getSchemaString(){

    }

//    @Test
//    public void test_getSchemaString(){
//        int id=1;
//        RestTemplate restTemplate =mock(RestTemplate.class);
//        ResponseEntity<SchemaString> mockRes =(ResponseEntity<SchemaString>)mock(ResponseEntity.class);
//        String regUrl="http://10.65.159.119:8081"+"/schemas/ids/"+id;
//        when(restTemplate.getForEntity(regUrl, SchemaString.class)).thenReturn(mockRes);
//        SchemaString result=new SchemaString();
//        when(mockRes.getBody()).thenReturn(result);
//        kafkaSchemaService.getSchemaString(id);
//    }

//    @Configuration
//    @ComponentScan("org.apache.griffin.core")
//    static class MyServiceConfiguration {
//        @Bean
//        PropertyPlaceholderConfigurer propConfig() {
//            PropertyPlaceholderConfigurer ppc =  new PropertyPlaceholderConfigurer();
//            ppc.setLocation(new ClassPathResource("application.properties"));
//            return ppc;
//        }
//    }
}
