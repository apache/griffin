package org.apache.griffin.core.metastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HiveMetastoreTest implements CommandLineRunner{
    private static final Logger log = LoggerFactory.getLogger(HiveMetastoreTest.class);
    public static void main(String[] args) {
        SpringApplication.run(HiveMetastoreTest.class, args);
    }


    public void run(String... strings) throws Exception {
        {


        }
    }
}
