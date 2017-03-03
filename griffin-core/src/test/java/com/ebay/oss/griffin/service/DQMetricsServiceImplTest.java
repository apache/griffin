package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.service.DQMetricsServiceImpl;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:context.xml"})
public class DQMetricsServiceImplTest {

    @Autowired
    private DQMetricsServiceImpl dqMetricsService;

    //insertMetadata
    private void testInsertMetadata(String id, String metric, float val) {
        DqMetricsValue dqmv = new DqMetricsValue();
        dqmv.set_id(12345678L);
        dqmv.setAssetId(id);
        dqmv.setMetricName(metric);
        dqmv.setTimestamp(new Date().getTime());
        dqmv.setValue(val);
        dqMetricsService.insertMetadata(dqmv);
    }

    //getLatestlMetricsbyId
    private void testGetLatestlMetricsbyId(String id, String metric, float val) {
        DqMetricsValue dv = dqMetricsService.getLatestlMetricsbyId(id);
        assertNotNull(dv);
        assertEquals(dv.getMetricName(), metric);
        assertTrue(dv.getValue() == val);
    }

    @Test
    public void testDQMetricsService() {

        String id = "test100", metric = "mean";
        float val = 4835.3f;

        //insertMetadata
        testInsertMetadata(id, metric, val);

        //getLatestlMetricsbyId
        testGetLatestlMetricsbyId(id, metric, val);
    }

    @Test
    public void testHeatMap() {
        System.out.println("===== Heat Map =====");
        List<SystemLevelMetrics> slmList = dqMetricsService.heatMap();
        for (SystemLevelMetrics slm : slmList) {
            System.out.println("--- " + slm.getName() + ": Dq: " + slm.getDq() + " ---");
            List<AssetLevelMetrics> almList = slm.getMetrics();
            for (AssetLevelMetrics alm : almList) {
                System.out.println(alm.getName() + ": " + alm.getMetricType() + " -> " + alm.getDq());
            }
            System.out.println();
        }
    }

    @Test
    public void testBriefMetrics() {
        System.out.println("===== Brief Metrics =====");
        List<SystemLevelMetrics> slmList1 = dqMetricsService.briefMetrics("Bullseye");
        for (SystemLevelMetrics slm : slmList1) {
            System.out.println("--- " + slm.getName() + ": Dq: " + slm.getDq() + " ---");
            List<AssetLevelMetrics> almList = slm.getMetrics();
            for (AssetLevelMetrics alm : almList) {
                System.out.println(alm.getName() + ": " + alm.getMetricType() + " -> " + alm.getDq());
            }
            System.out.println();
        }
    }

    @Test
    public void testDashBoard() {
        System.out.println("===== Dash Board =====");
        List<SystemLevelMetrics> slmList2 = dqMetricsService.dashboard("IDLS");
        for (SystemLevelMetrics slm : slmList2) {
            System.out.println("--- " + slm.getName() + ": Dq: " + slm.getDq() + " ---");
            List<AssetLevelMetrics> almList = slm.getMetrics();
            for (AssetLevelMetrics alm : almList) {
                System.out.println(alm.getName() + ": " + alm.getMetricType() + " -> " + alm.getDq());
            }
            System.out.println();
        }
    }

    @Test
    public void testOneDataCompleteDashboard() {
        System.out.println("===== oneDataCompleteDashboard =====");
        AssetLevelMetrics aslm = dqMetricsService.oneDataCompleteDashboard("test_accuracy_1");
        assertNotNull(aslm);
        System.out.println(aslm.getName() + " -> " + aslm.getDq());
        System.out.println();
    }

    @Test
    public void testOneDataBriefDashboard() {
        System.out.println("===== oneDataBriefDashboard =====");
        AssetLevelMetrics alm1 = dqMetricsService.oneDataBriefDashboard("test_accuracy_1");
        assertNotNull(alm1);
        System.out.println(alm1.getName() + " -> " + alm1.getDq());
        System.out.println();
    }

    @Test
    public void testMetricsForReport() {
        System.out.println("===== metricsForReport =====");
        AssetLevelMetrics alm2 = dqMetricsService.metricsForReport("test_accuracy_1");
        assertNotNull(alm2);
        System.out.println(alm2.getName() + " -> " + alm2.getDq());
        System.out.println();
    }

    @Test
    public void testUpdateLatestDQList() {
        System.out.println("===== updateLatestDQList =====");
        dqMetricsService.updateLatestDQList();
        System.out.println("update latest dq list succeed");
        System.out.println();
    }

    @Test
    public void testGetOverViewStats() {
        System.out.println("===== getOverViewStats =====");
        OverViewStatistics ovs = dqMetricsService.getOverViewStats();
        assertNotNull(ovs);
        System.out.println("metrics: " + ovs.getMetrics() + " assets: " + ovs.getAssets());
        System.out.println();
    }

    @Test
    public void testMydashboard() {
        System.out.println("===== My Dash Board =====");
        List<SystemLevelMetrics> slmList3 = dqMetricsService.mydashboard("lliu13");
        for (SystemLevelMetrics slm : slmList3) {
            System.out.println("--- " + slm.getName() + ": Dq: " + slm.getDq() + " ---");
            List<AssetLevelMetrics> almList = slm.getMetrics();
            for (AssetLevelMetrics alm : almList) {
                System.out.println(alm.getName() + ": " + alm.getMetricType() + " -> " + alm.getDq());
            }
            System.out.println();
        }
        if (slmList3.size() == 0) System.out.println("my dash board is empty");
    }

    private void testInsertSampleFilePath(String modelName, String path) {
        System.out.println("===== Insert Sample File Path =====");
        SampleFilePathLKP sfp = new SampleFilePathLKP();
        sfp.setModelName(modelName);
        sfp.setHdfsPath(path);
        sfp.setTimestamp(new Date().getTime());
        dqMetricsService.insertSampleFilePath(sfp);
        System.out.println("Insert Sample File Path");
    }

    private void testListSampleFile(String modelName) {
        System.out.println("===== List Sample File =====");
        List<SampleOut> soList = dqMetricsService.listSampleFile(modelName);
        Iterator<SampleOut> itr = soList.iterator();
        while (itr.hasNext()) {
            SampleOut so = itr.next();
            System.out.println("SampleOut: " + new Date(so.getDate()).toString() + " " + so.getPath());
        }
        if (soList.size() == 0) System.out.println("Sample File List is empty");
    }

    @Test
    public void testSampleFileProcess() {
        String modelName = "testModel100", path = "/user/b_des/bark/testPath/";

        //insert sample file
        testInsertSampleFilePath(modelName, path);

        //list sample file
        testListSampleFile(modelName);

    }

    @Test
    public void testGetThresholds() {
        Map<String, String> thresholdMap = dqMetricsService.getThresholds();
        System.out.println("---- threshold map ----");
        for (Map.Entry<String, String> me : thresholdMap.entrySet()) {
            System.out.println(me.getKey() + " -> " + me.getValue());
        }
    }

}
