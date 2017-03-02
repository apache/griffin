package org.apache.griffin.core.service;

import org.apache.griffin.core.repo.HiveDataAssetRepo;
import org.apache.griffin.core.spec.HiveDataAsset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



@RestController
public class GriffinController {
    @Autowired
    HiveDataAssetRepo repository;

    @RequestMapping("/version")
    public String greeting() {
        return "0.1.0";
    }

    @RequestMapping("/allhive")
    public Iterable<HiveDataAsset> getALLHiveDataAsset(){
        return repository.findAll();
    }
}

