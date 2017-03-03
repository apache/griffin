package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.service.DataAssetService;
import com.ebay.oss.griffin.vo.DataAssetInput;
import com.ebay.oss.griffin.vo.PlatformMetadata;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class DataAssetServiceTest {
    @Autowired
    private DataAssetService dataAssetService;

    private String assetName = "testAsset_";
    private String system = "Bullseye";
    private String owner = "lliu13";
    private String newOwner = "wenzhao";

    @Test
    public void testGetSourceTree() {
        List<PlatformMetadata> pmdList = dataAssetService.getSourceTree();
        assertNotNull(pmdList);
    }

    private List<DataAsset> testGetAllDataAssets() {
        return dataAssetService.getAllDataAssets();
    }

    private DataAssetInput testCreateDataAsset() {
        try {
            DataAssetInput newData = new DataAssetInput();
            newData.setAssetName(assetName + new Date().getTime());
            newData.setAssetType("hdfsfile");
            newData.setSystem(system);
            newData.setAssetHDFSPath("/user/b_des/bark/");
            newData.setPlatform("Apollo");
            newData.setOwner(owner);
            int success = dataAssetService.createDataAsset(newData);
            assertEquals(success, 0);
            System.out.println("add new data asset success");
            return newData;
        } catch (Exception e) {
            System.out.println("fail to add new data asset");
            e.printStackTrace();
            return null;
        }
    }

    private long getDataAssetByName(List<DataAsset> daList, String name) {
        long newId = -1L;
        Iterator<DataAsset> itr = daList.iterator();
        while (itr.hasNext()) {
            DataAsset da = itr.next();
            if (da.getAssetName().equals(name)) {
                newId = da.get_id();
                break ;
            }
        }
        assertTrue(newId > 0);
        System.out.println("get new add data asset id: " + newId);
        return newId;
    }

    private void testGetDataAsset(long id, String sys, String onr) {
        try {
            //get that data asset
            DataAsset tda = dataAssetService.getDataAssetById(id);
            assertNotNull(tda);
            assertEquals(tda.getSystem(), sys);
            assertEquals(tda.getOwner(), onr);
            System.out.println("find id " + id + " success");
        } catch (Exception e) {
            System.out.println("fail to find data asset");
            e.printStackTrace();
        }
    }

    private void testGetNonExistDataAsset(long id) {
        try {
            DataAsset tda = dataAssetService.getDataAssetById(id);
            assertNull(tda);
            System.out.println("can not find non-exist data asset");
        } catch (Exception e) {
            System.out.println("fail to find non-exist data asset");
            e.printStackTrace();
        }
    }

    private void updateDataAsset(DataAssetInput da, String newOwner) {
        try {
            da.setOwner(newOwner);
            dataAssetService.updateDataAsset(da);
            System.out.println("update data asset success");
        } catch (Exception e) {
            System.out.println("fail to update data asset");
            e.printStackTrace();
        }
    }

    private void testRemoveDataAsset(long id) {
        try {
            dataAssetService.removeAssetById(id);

            DataAsset tda2 = dataAssetService.getDataAssetById(id);
            assertNull(tda2);
            System.out.println("remove the data asset " + id + " success");
        } catch (Exception e) {
            System.out.println("fail to remove data asset");
            e.printStackTrace();
        }
    }

    @Test
    public void testDataAssetService(){

        //get all data
        List<DataAsset> daList = testGetAllDataAssets();
        System.out.println("current data assets count: " + daList.size());

        //add new dataAsset
        DataAssetInput newData = testCreateDataAsset();
        assertNotNull(newData);

        //get all data again
        List<DataAsset> daList1 = testGetAllDataAssets();
        assertTrue(daList1.size() - daList.size() == 1);
        System.out.println("current data assets count: " + daList1.size());

        //find new data asset id
        long id = getDataAssetByName(daList1, assetName);
        testGetDataAsset(id, system, owner);

        //find non-exist data asset
        testGetNonExistDataAsset(863510L);

        //update exist data asset
        updateDataAsset(newData, newOwner);
        testGetDataAsset(id, system, newOwner);

        //update non-exist data asset
        DataAssetInput newData1 = new DataAssetInput();
        newData1.setAssetName("test_newDataAsset");
        newData1.setAssetType("hdfsfile");
        newData1.setSystem(system);
        newData1.setAssetHDFSPath("/user/b_des/bark/");
        newData1.setPlatform("Apollo");
        newData1.setOwner(owner);
        updateDataAsset(newData1, newOwner);

        //remove the data asset
        testRemoveDataAsset(id);

    }
}

