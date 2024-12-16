package org.apache.griffin.dqc;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DQCTriggerService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job dqcFlow;

    @GetMapping("/run")
    public ResponseEntity<String> runDqcFlow(@RequestParam(required = false) String param) {
        try {
            // Create JobParameters (if any) or pass empty parameters
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("requestParam", param != null ? param : "default_value:"+System.currentTimeMillis())
                    .toJobParameters();

            // Launch the job
            jobLauncher.run(dqcFlow, jobParameters);

            return ResponseEntity.ok("DQC Flow job started successfully.");
        } catch (Throwable e) {
            return ResponseEntity.status(500).body("Job failed to start: " + e.getMessage());
        }
    }
}

