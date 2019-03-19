package org.apache.griffin.core.job;

import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.entity.AbstractJob;
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.apache.griffin.core.job.repo.JobRepo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.quartz.*;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

import static org.apache.griffin.core.util.EntityMocksHelper.createGriffinJob;
import static org.apache.griffin.core.util.EntityMocksHelper.createJobInstance;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(SpringRunner.class)
public class JobServiceImplTest {

    @Mock
    private JobRepo<AbstractJob> jobRepo;

    @Mock
    private SchedulerFactoryBean factory;

    @Mock
    private JobInstanceRepo instanceRepo;

    @InjectMocks
    private JobServiceImpl jobService;


    @Test
    public void testTriggerJobById() throws SchedulerException {
        Long jobId = 1L;
        AbstractJob job = createGriffinJob();
        given(jobRepo.findByIdAndDeleted(jobId,false)).willReturn(job);
        Scheduler scheduler = mock(Scheduler.class);
        given(scheduler.checkExists(any(JobKey.class))).willReturn(true);
        ListenerManager listenerManager = mock(ListenerManager.class);
        given(scheduler.getListenerManager()).willReturn(listenerManager);
        doNothing().when(listenerManager).addTriggerListener(any(CountDownTriggerListener.class), any(Matcher.class));
        given(factory.getScheduler()).willReturn(scheduler);
        JobInstanceBean jobInstanceBean = createJobInstance();
        given(instanceRepo.findByTriggerKey(anyString())).willReturn(Collections.singletonList(jobInstanceBean));

        JobInstanceBean result = jobService.triggerJobById(jobId, 0L);

        assertEquals(result.getAppId(), jobInstanceBean.getAppId());
        assertEquals(result.getTms(), result.getTms());
        verify(scheduler, times(1)).scheduleJob(any());
        verify(listenerManager, times(1)).addTriggerListener(any(CountDownTriggerListener.class), any(Matcher.class));
    }


    @Test(expected = GriffinException.NotFoundException.class)
    public void testTriggerJobByIdFail() throws SchedulerException {
        Long jobId = 1L;
        given(jobRepo.findByIdAndDeleted(jobId,false)).willReturn(null);
        jobService.triggerJobById(jobId, 0L);
    }
}
