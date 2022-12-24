package org.apache.griffin.core.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GriffinMasterMain {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(GriffinMasterMain.class);

    public static void main(String[] args) {
        SpringApplication.run(GriffinMasterMain.class, args);
        LOGGER.info("application started");
    }
}
