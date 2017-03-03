package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.domain.AnomalyType;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.ScheduleType;
import com.ebay.oss.griffin.domain.SystemType;
import com.ebay.oss.griffin.domain.ValidityType;
import com.ebay.oss.griffin.service.DqModelService;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelBasicInputNew;
import com.ebay.oss.griffin.vo.ModelExtraInputNew;
import com.ebay.oss.griffin.vo.ModelInput;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class DqModelServiceTest {

    @Autowired
    private DqModelService dqModelService;

    private String[] newModelName = {"testAccu", "testVali", "testAnom", "testPub"};
    private String testOwner = "lliu13";
    private String testPlatform = "Apollo";

    public void testNewModel(int type) {
        DqModel ret = null;

        if (type == ModelType.ACCURACY) {
            ModelInput tempModel = new ModelInput();
            ModelBasicInputNew basic = new ModelBasicInputNew();
            ModelExtraInputNew extra = new ModelExtraInputNew();
            basic.setDataaset("rheos_view_event");
            basic.setDataasetId(16);
            basic.setDesc("accu for rheos_view_event");
            basic.setEmail(testOwner + "@ebay.com");
            basic.setName(newModelName[0]);
            basic.setOwner(testOwner);
            basic.setScheduleType(ScheduleType.DAILY);
            basic.setStatus(ModelStatus.DEPLOYED);
            basic.setSystem(SystemType.KAFKA);
            basic.setType(type);
            extra.setVaType(ValidityType.TOTAL_COUNT);
            extra.setColumn("u_id");
            extra.setSrcDataSet("Sojourner.soj_view_event");
            extra.setSrcDb(testPlatform);
            extra.setTargetDataSet("Kafka.rheos_view_event");
            extra.setTargetDb(testPlatform);
            tempModel.setBasic(basic);
            tempModel.setExtra(extra);
            ret = dqModelService.newModel(tempModel);
        } else if (type == ModelType.VALIDITY) {
            ModelInput tempModel = new ModelInput();
            ModelBasicInputNew basic = new ModelBasicInputNew();
            ModelExtraInputNew extra = new ModelExtraInputNew();
            basic.setDataaset("dmg");
            basic.setDataasetId(6);
            basic.setDesc("total count for dmg");
            basic.setEmail(testOwner + "@ebay.com");
            basic.setName(newModelName[1]);
            basic.setOwner(testOwner);
            basic.setScheduleType(ScheduleType.DAILY);
            basic.setStatus(ModelStatus.DEPLOYED);
            basic.setSystem(SystemType.BULLSEYE);
            basic.setType(type);
            extra.setVaType(ValidityType.TOTAL_COUNT);
            extra.setColumn("age");
            extra.setSrcDataSet("Bullseye.dmg");
            extra.setSrcDb(testPlatform);
            tempModel.setBasic(basic);
            tempModel.setExtra(extra);
            ret = dqModelService.newModel(tempModel);
        } else if (type == ModelType.ANOMALY) {
            ModelInput tempModel = new ModelInput();
            ModelBasicInputNew basic = new ModelBasicInputNew();
            ModelExtraInputNew extra = new ModelExtraInputNew();
            basic.setDataaset("dmg");
            basic.setDataasetId(6);
            basic.setDesc("anomaly for dmg");
            basic.setEmail(testOwner + "@ebay.com");
            basic.setName(newModelName[2]);
            basic.setOwner(testOwner);
            basic.setScheduleType(ScheduleType.DAILY);
            basic.setStatus(ModelStatus.DEPLOYED);
            basic.setSystem(SystemType.BULLSEYE);
            basic.setType(type);
            extra.setAnType(AnomalyType.ANOMALY_DEFAULT);
            extra.setColumn("age");
            extra.setSrcDataSet("Bullseye.dmg");
            extra.setSrcDb(testPlatform);
            tempModel.setBasic(basic);
            tempModel.setExtra(extra);
            ret = dqModelService.newModel(tempModel);
        } else if (type == ModelType.PUBLISH) {
            ModelInput tempModel = new ModelInput();
            ModelBasicInputNew basic = new ModelBasicInputNew();
            ModelExtraInputNew extra = new ModelExtraInputNew();
            basic.setDataaset("dmg");
            basic.setDataasetId(6);
            basic.setDesc("publish metrics for dmg");
            basic.setEmail(testOwner + "@ebay.com");
            basic.setName(newModelName[3]);
            basic.setOwner(testOwner);
            basic.setScheduleType(ScheduleType.DAILY);
            basic.setStatus(ModelStatus.DEPLOYED);
            basic.setSystem(SystemType.BULLSEYE);
            basic.setType(type);
            extra.setPublishUrl("http://www.test.com");
            extra.setColumn("age");
            extra.setSrcDataSet("Bullseye.dmg");
            extra.setSrcDb(testPlatform);
            tempModel.setBasic(basic);
            tempModel.setExtra(extra);
            ret = dqModelService.newModel(tempModel);
        }
        assertNotNull(ret);
    }

    private List<DqModelVo> testGetAllModels() {
        return dqModelService.getAllModles();
    }

    private void testGetModels() {
        try {
            //find success
            for (int i = 1; i < 4; i++) {
                ModelInput mi = dqModelService.getModelByName(newModelName[i]);
                if (i == 1 || i == 2) {
                    assertEquals(mi.getBasic().getOwner(), testOwner);
                    assertEquals(mi.getExtra().getSrcDb(), testPlatform);
                    System.out.println("get model by name: " + mi.getBasic().getName()
                            + " with owner: " + mi.getBasic().getOwner()
                            + " and platform: " + mi.getExtra().getSrcDb());
                } else {
                    assertEquals(mi.getBasic().getOwner(), testOwner);
                    assertEquals(mi.getExtra().getPublishUrl(), "http://www.test.com");
                    System.out.println("get model by name: " + mi.getBasic().getName()
                            + " with owner: " + mi.getBasic().getOwner()
                            + " and publish url: " + mi.getExtra().getPublishUrl());
                }
            }
            //find fail
            ModelInput mi = dqModelService.getModelByName("findfailmodel");
            assertNull(mi);
        } catch (Exception e) {
            System.out.println("fail to get model");
            e.printStackTrace();
        }
    }

    private void testDeleteModel(String name) {
        try {
            int del = dqModelService.deleteModel(name);
            assertTrue(del <= 0);
            System.out.println("delete model success");
        } catch (Exception e) {
            System.out.println("fail to delete model");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testDQModelService(){

        //get all models
        List<DqModelVo> modelsList = testGetAllModels();
        System.out.println("current models count: " + modelsList.size());

        //add new models success
        testNewModel(ModelType.ACCURACY);
        testNewModel(ModelType.VALIDITY);
        testNewModel(ModelType.ANOMALY);
        testNewModel(ModelType.PUBLISH);

        //get all models
        List<DqModelVo> modelsList1 = testGetAllModels();
        assertTrue(modelsList1.size() - modelsList.size() == 4);
        System.out.println("current models count: " + modelsList1.size());

        //add the same new model fail
        testNewModel(ModelType.VALIDITY);

        //find new add model
        testGetModels();

        //delete models
        //TODO:: move to tearDown
        for (int i = 0; i < newModelName.length; i++){
            testDeleteModel(newModelName[i]);
        }
    }

    @Test
    public void testGtGeneralModeltAndEnableSchedule4Model() {
        String name = "test_accuracy_1";
        DqModel me = dqModelService.getGeneralModel(name);
        assertNotNull(me);

        dqModelService.enableSchedule4Model(me);
    }
}
