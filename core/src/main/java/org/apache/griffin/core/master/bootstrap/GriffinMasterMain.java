package org.apache.griffin.core.master.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@ComponentScan({"org.apache.griffin.core.master", "org.apache.griffin.core.common"})
//@ComponentScan({"org.apache.griffin.core.master.service"})
@SpringBootApplication
@EnableScheduling
public class GriffinMasterMain {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(GriffinMasterMain.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(GriffinMasterMain.class, args);
        LOGGER.info("application started");

        Object taskManagerServiceImpl = run.getBean("taskManagerServiceImpl");
        System.out.println(taskManagerServiceImpl);
    }
}
