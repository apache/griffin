package org.apache.griffin.dqc;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;


@Configuration
@EnableBatchProcessing
public class DQCFlowFactory {
    Logger logger = LoggerFactory.getLogger(DQCFlowFactory.class);

    @Autowired
    public JobRepository jobRepository;

    @Autowired
    public PlatformTransactionManager transactionManager;


    /**
     * This Job defines the one typical DQC workflow as below.
     * <pre>
     *                  -> FetchSourceCount
     *                /                     \
     * PrepareStep -->                        --> CompareStep
     *                \                     /
     *                  -> FetchTargetCount
     * </pre>
     */
    @Bean
    public Job Split_Apply_Combine(JobRepository jobRepository, Step setup, Step scanSource, Step scanTarget, Step check) {

        Flow f2 = new FlowBuilder<SimpleFlow>("source")
                .start(scanSource)
                .build();

        Flow f3 = new FlowBuilder<SimpleFlow>("target")
                .start(scanTarget)
                .build();

        Flow splitFlow = new FlowBuilder<SimpleFlow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor()) // Split execution in parallel
                .add(f2, f3)
                .build();

        Flow sequentialFlow = new FlowBuilder<SimpleFlow>("sequentialFlow")
                .start(setup)
                .next(splitFlow)
                .next(check)
                .build();

        // Define the job using the sequential flow
        return new JobBuilder("CompareTwoAssetsJob", jobRepository)
                .start(sequentialFlow)
                .end()
                .build();
    }

}
