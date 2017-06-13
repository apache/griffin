package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiangrchen on 6/7/17.
 */
@RunWith(SpringRunner.class)
public class MetricServiceImplTest {
    @TestConfiguration
    static class MetricServiceConfiguration{
        @Bean
        public MetricServiceImpl service(){
            return new MetricServiceImpl();
        }
    }

    @MockBean
    private MeasureRepo measureRepo;

    @Autowired
    private MetricServiceImpl service;

    @Before
    public void setup(){
    }

    @Test
    public void testGetOrgByMeasureName(){
        try {
            String measureName="viewitem_hourly";
            String tmp = service.getOrgByMeasureName(measureName);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get org by measure name viewitem_hourly");
        }
    }
}
