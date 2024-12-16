package org.apache.griffin.dqc;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Random;

/**
 * Split-Apply-Combine Flow
 */
@Component
public class SACFlow implements IDQCFlow{
    private final Random random;

    public SACFlow() {
        this.random = new Random();
    }


    @Bean
    public Step setup(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("setup", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().put("setup_key", "setup_value");
                    Thread.sleep(random.nextInt(3000));
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .listener(executionContextPromotionListener()) // Add listener
                .build();
    }

    public ExecutionContextPromotionListener executionContextPromotionListener() {
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{"setup_key", "source_count", "target_count"});

        return listener;
    }

    @Bean
    public Step scanSource(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("scanSource", jobRepository).tasklet((contribution, chunkContext) -> {
                    System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread() + ": running scanSource");
                    String setupKey = (String) chunkContext.getStepContext().getJobExecutionContext().get("setup_key");
                    System.out.println("setup_key from setup: " + setupKey);
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().put("source_count", 100);
                    Thread.sleep(random.nextInt(3000));
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .listener(executionContextPromotionListener()) // Add listener
                .build();
    }

    @Bean
    public Step scanTarget(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("scanTarget", jobRepository).tasklet((contribution, chunkContext) -> {
                    System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread() + ": running scanTarget");
                    Thread.sleep(random.nextInt(3000));
                    System.out.println(chunkContext.getStepContext().getJobExecutionContext().get("setup_key"));
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().put("target_count", 100);
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .listener(executionContextPromotionListener()) // Add listener
                .build();
    }

    //    @Qualifier("step1")
    @Bean
    public Step check(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("check", jobRepository).tasklet((contribution, chunkContext) -> {
            System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread() + ": running check");
            Thread.sleep(random.nextInt(3000));
            System.out.println(chunkContext.getStepContext().getJobExecutionContext().get("setup_key"));
            int source_count = (int) chunkContext.getStepContext().getJobExecutionContext().get(("source_count"));
            int target_count = (int) chunkContext.getStepContext().getJobExecutionContext().get(("target_count"));
            if (source_count == target_count) {
                System.out.println("source_count == target_count");
            } else {
                System.out.println("source_count != target_count");
            }
            return RepeatStatus.FINISHED;
        }, transactionManager).build();
    }


}
