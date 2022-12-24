package org.apache.griffin.core.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GriffinWorkerMain {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(GriffinWorkerMain.class);

    public static void main(String[] args) {
        SpringApplication.run(GriffinWorkerMain.class, args);
        LOGGER.info("application started");
    }
}
