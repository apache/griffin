package com.ebay.oss.griffin.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.service.DqScheduleService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class DqScheduleServiceTest {
    @Autowired
    private DqScheduleService dqJobSchedulingService;

    @Test
    public void testSchedulingJobs(){

        dqJobSchedulingService.schedulingJobs();
        System.out.println("scheduling jobs success");

    }
}
