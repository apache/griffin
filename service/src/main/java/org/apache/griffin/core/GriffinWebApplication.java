package org.apache.griffin.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.griffin.core.repo.HiveDataAssetRepo;
import org.apache.griffin.core.spec.HiveDataAsset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;

@SpringBootApplication
public class GriffinWebApplication implements CommandLineRunner{
    private static final Logger log = LoggerFactory.getLogger(GriffinWebApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(GriffinWebApplication.class, args);
    }

    @Autowired
    HiveDataAssetRepo repository;


    public void run(String... strings) throws Exception {
        {

            repository.save(new HiveDataAsset("table1","BE", "wenzhao"));
            repository.save(new HiveDataAsset("table2","BE", "wenzhao"));
            repository.save(new HiveDataAsset("table3","PDS", "wenzhao"));
            repository.save(new HiveDataAsset("table4","PDS", "wenzhao"));
            repository.save(new HiveDataAsset("table5","IDS", "wenzhao"));
            repository.save(new HiveDataAsset("table6","IDS", "wenzhao"));
            repository.save(new HiveDataAsset("table7","GRIFFIN", "yueguo"));

            log.info("-------------------------------");
            for (HiveDataAsset hda : repository.findAll()) {
                log.info(hda.toString());
            }

        }
    }
}
