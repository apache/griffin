package com.ebay.oss.griffin.vo;

import com.ebay.oss.griffin.domain.*;
import com.ebay.oss.griffin.vo.AccuracyHiveJobConfig;
import com.ebay.oss.griffin.vo.AccuracyHiveJobConfigDetail;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.AssetLevelMetricsDetail;
import com.ebay.oss.griffin.vo.BaseObj;
import com.ebay.oss.griffin.vo.BollingerBandsEntity;
import com.ebay.oss.griffin.vo.DQHealthStats;
import com.ebay.oss.griffin.vo.DataAssetIndex;
import com.ebay.oss.griffin.vo.DataAssetInput;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.LoginUser;
import com.ebay.oss.griffin.vo.MADEntity;
import com.ebay.oss.griffin.vo.MappingItemInput;
import com.ebay.oss.griffin.vo.ModelBasicInputNew;
import com.ebay.oss.griffin.vo.ModelExtraInputNew;
import com.ebay.oss.griffin.vo.ModelInput;
import com.ebay.oss.griffin.vo.NotificationRecord;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.PartitionConfig;
import com.ebay.oss.griffin.vo.PlatformMetadata;
import com.ebay.oss.griffin.vo.PlatformSubscription;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;
import com.ebay.oss.griffin.vo.SystemLevelMetricsList;
import com.ebay.oss.griffin.vo.SystemMetadata;
import com.ebay.oss.griffin.vo.SystemSubscription;
import com.ebay.oss.griffin.vo.ValidateHiveJobConfig;
import com.ebay.oss.griffin.vo.ValidateHiveJobConfigLv1Detail;
import com.ebay.oss.griffin.vo.ValidateHiveJobConfigLv2Detail;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class VoResourceTest {

    @Test
    public void testAccuracyHiveJobConfigDetail() {
        AccuracyHiveJobConfigDetail detail = new AccuracyHiveJobConfigDetail();

        detail.setSourceColId(1);
        assertEquals(1, detail.getSourceColId());
        detail.setSourceColName("source");
        assertEquals("source", detail.getSourceColName());
        detail.setTargetColId(2);
        assertEquals(2, detail.getTargetColId());
        detail.setTargetColName("target");
        assertEquals("target", detail.getTargetColName());
        detail.setMatchFunction("function");
        assertEquals("function", detail.getMatchFunction());
        detail.setPK(true);
        assertEquals(true, detail.isPK());
    }

    @Test
    public void testPartitionConfig() {
        PartitionConfig detail = new PartitionConfig();

        detail.setColName("name");
        assertEquals("name", detail.getColName());
        detail.setColValue("value");
        assertEquals("value", detail.getColValue());
    }

    @Test
    public void testAccuracyHiveJobConfig() {
        AccuracyHiveJobConfig config = new AccuracyHiveJobConfig();
        AccuracyHiveJobConfig config1 = new AccuracyHiveJobConfig("source", "target");

        config.setSource("source");
        assertEquals("source", config.getSource());
        config.setTarget("target");
        assertEquals("target", config.getTarget());

        List<AccuracyHiveJobConfigDetail> detailList = new ArrayList<AccuracyHiveJobConfigDetail>();
        detailList.add(new AccuracyHiveJobConfigDetail(1, "src", 2, "tgt", "func", false));
        config.setAccuracyMapping(detailList);
        assertEquals(detailList, config.getAccuracyMapping());

        List<PartitionConfig> srcPtConfigList = new ArrayList<PartitionConfig>();
        srcPtConfigList.add(new PartitionConfig("name", "value"));
        config.setSrcPartitions(srcPtConfigList);
        assertEquals(srcPtConfigList, config.getSrcPartitions());

        List<PartitionConfig> l1 = new ArrayList<PartitionConfig>();
        List<List<PartitionConfig>> tgtPtConfigList = new ArrayList<List<PartitionConfig>>();
        tgtPtConfigList.add(l1);
        config.setTgtPartitions(tgtPtConfigList);
        assertEquals(tgtPtConfigList, config.getTgtPartitions());
    }

    @Test
    public void testAssetLevelMetrics() {
        AssetLevelMetrics metrics = new AssetLevelMetrics();
        AssetLevelMetrics metrics1 = new AssetLevelMetrics("name");
        AssetLevelMetrics metrics2 = new AssetLevelMetrics("name", 12.3f, 12345L);
        AssetLevelMetrics metrics3 = new AssetLevelMetrics("name", "type", 12.3f, 12345L, 30);


        metrics.setName("name");
        assertEquals("name", metrics.getName());
        metrics.setDq(12.3f);
        assertTrue(12.3f == metrics.getDq());
        metrics.setTimestamp(12345L);
        assertEquals(12345L, metrics.getTimestamp());
        metrics.setDqfail(30);
        assertEquals(30, metrics.getDqfail());

        List<AssetLevelMetricsDetail> detailList = new ArrayList<AssetLevelMetricsDetail>();
        metrics.setDetails(detailList);
        metrics.addAssetLevelMetricsDetail(new AssetLevelMetricsDetail());
        assertEquals(detailList, metrics.getDetails());

        metrics.setMetricType("type");
        assertEquals("type", metrics.getMetricType());
        metrics.setAssetName("assetName");
        assertEquals("assetName", metrics.getAssetName());

        AssetLevelMetrics metrics4 = new AssetLevelMetrics(metrics, 2);
    }

    @Test
    public void testAssetLevelMetricsDetail() {
        AssetLevelMetricsDetail detail = new AssetLevelMetricsDetail();

        detail.setTimestamp(12345L);
        assertEquals(12345L, detail.getTimestamp());
        detail.setValue(12.3f);
        assertTrue(12.3f == detail.getValue());

        BollingerBandsEntity bbe = new BollingerBandsEntity(200L, 100L, 150L);
        detail.setBolling(bbe);
        assertEquals(bbe, detail.getBolling());

        detail.setComparisionValue(12.3f);
        assertTrue(12.3f == detail.getComparisionValue());

        MADEntity mad = new MADEntity(200L, 100L);
        detail.setMAD(mad);
        assertEquals(mad, detail.getMAD());

        AssetLevelMetricsDetail detail1 = new AssetLevelMetricsDetail(12345L, 25.3f);
        assertEquals(0, detail1.compareTo(detail));

        detail1.setTimestamp(12346L);
        assertEquals(-1, detail1.compareTo(detail));
        detail1.setTimestamp(12344L);
        assertEquals(1, detail1.compareTo(detail));

        AssetLevelMetricsDetail detail2 = new AssetLevelMetricsDetail(12345L, 25.3f, bbe);
        AssetLevelMetricsDetail detail3 = new AssetLevelMetricsDetail(12345L, 25.3f, 12.3f);
        AssetLevelMetricsDetail detail4 = new AssetLevelMetricsDetail(12345L, 25.3f, mad);
        AssetLevelMetricsDetail detail5 = new AssetLevelMetricsDetail(bbe);
        AssetLevelMetricsDetail detail6 = new AssetLevelMetricsDetail(mad);
        AssetLevelMetricsDetail detail7 = new AssetLevelMetricsDetail(detail);
        AssetLevelMetricsDetail detail8 = new AssetLevelMetricsDetail(12.3f);
    }

    @Test
    public void testBaseObj() {
        BaseObj base = new BaseObj();
        assertNull(base.validate());
    }

    @Test
    public void testBollingerBandsEntity() {
        BollingerBandsEntity entity = new BollingerBandsEntity();

        entity.setUpper(300L);
        assertEquals(300L, entity.getUpper());
        entity.setLower(100L);
        assertEquals(100L, entity.getLower());
        entity.setMean(200L);
        assertEquals(200L, entity.getMean());
    }

    @Test
    public void testDataAssetIndex() {
        DataAssetIndex dai = new DataAssetIndex();

        dai.setId(12345L);
        assertEquals(new Long(12345L), dai.getId());
        dai.setName("name");
        assertEquals("name", dai.getName());
    }

    @Test
    public void testDataAssetInput() {
        DataAssetInput input = new DataAssetInput();

        input.setAssetName("aname");
        assertEquals("aname", input.getAssetName());
        input.setAssetType("atype");
        assertEquals("atype", input.getAssetType());
        input.setAssetHDFSPath("apath");
        assertEquals("apath", input.getAssetHDFSPath());
        input.setSystem("system");
        assertEquals("system", input.getSystem());
        input.setPlatform("platform");
        assertEquals("platform", input.getPlatform());

        List<DataSchema> dsList = new ArrayList<DataSchema>();
        dsList.add(new DataSchema("sname", "stype", "sdesc", "ssample"));
        input.setSchema(dsList);
        assertEquals(dsList, input.getSchema());

        input.setPartition("patition");
        assertEquals("patition", input.getPartition());

        List<PartitionFormat> pfList = new ArrayList<PartitionFormat>();
        pfList.add(new PartitionFormat("pfname", "pfformat"));
        input.setPartitions(pfList);
        assertEquals(pfList, input.getPartitions());

        input.setOwner("owner");
        assertEquals("owner", input.getOwner());
    }

    @Test
    public void testDQHealthStats() {
        DQHealthStats stats = new DQHealthStats();

        stats.setHealth(20);
        assertEquals(20, stats.getHealth());
        stats.setWarn(30);
        assertEquals(30, stats.getWarn());
        stats.setInvalid(10);
        assertEquals(10, stats.getInvalid());
    }

    @Test
    public void testDqModelVo() {
        DqModelVo vo = new DqModelVo();

        vo.setAssetName("assetname");
        assertEquals("assetname", vo.getAssetName());
        vo.setName("name");
        assertEquals("name", vo.getName());
        vo.setSystem(3);
        assertEquals(3, vo.getSystem());
        vo.setDescription("desc");
        assertEquals("desc", vo.getDescription());
        vo.setType(2);
        assertEquals(2, vo.getType());
        vo.setOwner("owner");
        assertEquals("owner", vo.getOwner());
        Date date = new Date();
        vo.setCreateDate(date);
        assertEquals(date, vo.getCreateDate());
        vo.setStatus("status");
        assertEquals("status", vo.getStatus());
    }

    @Test
    public void testLoginUser() {
        LoginUser user = new LoginUser();

        user.setUsername("name");
        assertEquals("name", user.getUsername());
        user.setPassword("pwd");
        assertEquals("pwd", user.getPassword());

        LoginUser user1 = new LoginUser("name1", "pwd1");
    }

    @Test
    public void testMADEntity() {
        MADEntity mad = new MADEntity();

        mad.setUpper(300L);
        assertEquals(300L, mad.getUpper());
        mad.setLower(100L);
        assertEquals(100L, mad.getLower());

        MADEntity mad1 = new MADEntity(300L, 100L);

        MADEntity mad2 = mad1.clone();
        assertEquals(300L, mad2.getUpper());
        assertEquals(100L, mad2.getLower());
    }

    @Test
    public void testMappingItemInput() {
        MappingItemInput input = new MappingItemInput();

        input.setTarget("target");
        assertEquals("target", input.getTarget());
        input.setSrc("source");
        assertEquals("source", input.getSrc());
        input.setIsPk(true);
        assertEquals(true, input.isIsPk());
        input.setMatchMethod("matchMethod");
        assertEquals("matchMethod", input.getMatchMethod());

        MappingItemInput input1 = new MappingItemInput("target", "source", true, "matchMethod");
    }

    @Test
    public void testModelBasicInputNew() {
        ModelBasicInputNew inputNew = new ModelBasicInputNew();

        inputNew.setType(1);
        assertEquals(1, inputNew.getType());
        inputNew.setSystem(2);
        assertEquals(2, inputNew.getSystem());
        inputNew.setScheduleType(3);
        assertEquals(3, inputNew.getScheduleType());
        inputNew.setOwner("owner");
        assertEquals("owner", inputNew.getOwner());
        inputNew.setName("name");
        assertEquals("name", inputNew.getName());
        inputNew.setDesc("desc");
        assertEquals("desc", inputNew.getDesc());
        inputNew.setEmail("email");
        assertEquals("email", inputNew.getEmail());
        inputNew.setDataaset("dataAsset");
        assertEquals("dataAsset", inputNew.getDataaset());
        inputNew.setDataasetId(12345L);
        assertEquals(12345L, inputNew.getDataasetId());
        inputNew.setThreshold(20.2f);
        assertTrue(20.2f == inputNew.getThreshold());
        inputNew.setStatus(1);
        assertEquals(1, inputNew.getStatus());
        inputNew.setStarttime(12345L);
        assertEquals(12345L, inputNew.getStarttime());
    }

    @Test
    public void testModelExtraInputNew() {
        ModelExtraInputNew inputNew = new ModelExtraInputNew();

        inputNew.setSrcDb("db");
        assertEquals("db", inputNew.getSrcDb());
        inputNew.setSrcDataSet("dataSet");
        assertEquals("dataSet", inputNew.getSrcDataSet());
        inputNew.setTargetDb("db");
        assertEquals("db", inputNew.getTargetDb());
        inputNew.setTargetDataSet("dataSet");
        assertEquals("dataSet", inputNew.getTargetDataSet());
        inputNew.setVaType(1);
        assertEquals(1, inputNew.getVaType());
        inputNew.setColumn("col");
        assertEquals("col", inputNew.getColumn());
        inputNew.setPublishUrl("url");
        assertEquals("url", inputNew.getPublishUrl());
        inputNew.setAnType(2);
        assertEquals(2, inputNew.getAnType());
    }

    @Test
    public void testModelInput() {
        ModelInput input = new ModelInput();

        ModelBasicInputNew basic = new ModelBasicInputNew();
        input.setBasic(basic);
        assertEquals(basic, input.getBasic());

        ModelExtraInputNew extra = new ModelExtraInputNew();
        input.setExtra(extra);
        assertEquals(extra, input.getExtra());

        List<MappingItemInput> mappings = new ArrayList<MappingItemInput>();
        MappingItemInput mi = new MappingItemInput("target", "source", true, "matchMethod");
        mappings.add(mi);
        input.setMappings(mappings);
        assertEquals(mappings, input.getMappings());

        ModelInput input1 = new ModelInput(basic, extra, mappings);

        ModelInput input2 = new ModelInput();
        String content = "srcDb|srcDataSet|targetDb|targetDataSet|id,uid,true,null;name,uname,false,null;age,age,false,null";
        input2.parseFromString(content);
        assertEquals("srcDb", input2.getExtra().getSrcDb());
        assertEquals("srcDataSet", input2.getExtra().getSrcDataSet());
        assertEquals("targetDb", input2.getExtra().getTargetDb());
        assertEquals("targetDataSet", input2.getExtra().getTargetDataSet());
        assertEquals(3, input2.getMappings().size());

        //field basic has notNull field name, so the validate result can not be null
        assertNotNull(input2.validate());
    }

    @Test
    public void testNotificationRecord() {
        NotificationRecord rcd = new NotificationRecord();

        rcd.setName("name");
        assertEquals("name", rcd.getName());
        rcd.setId(12);
        assertEquals(12, rcd.getId());
        rcd.setTimestamp(12345L);
        assertEquals(12345L, rcd.getTimestamp());
        rcd.setOwner("owner");
        assertEquals("owner", rcd.getOwner());
        rcd.setOperation("opr");
        assertEquals("opr", rcd.getOperation());
        rcd.setTarget("target");
        assertEquals("target", rcd.getTarget());
        rcd.setLink("link");
        assertEquals("link", rcd.getLink());

        NotificationRecord rcd1 = new NotificationRecord(12345L, "owner", "opr", "target", "name");
    }

    @Test
    public void testOverViewStatistics() {
        OverViewStatistics stat = new OverViewStatistics();

        stat.setAssets(2);
        assertEquals(2, stat.getAssets());
        stat.setMetrics(4);
        assertEquals(4, stat.getMetrics());
        DQHealthStats dqs = new DQHealthStats();
        stat.setStatus(dqs);
        assertEquals(dqs, stat.getStatus());
    }

    @Test
    public void testPlatformMetadata() {
        PlatformMetadata metaData = new PlatformMetadata();

        metaData.setId("id");
        assertEquals("id", metaData.getId());
        metaData.setPlatform("platform");
        assertEquals("platform", metaData.getPlatform());

        List<SystemMetadata> list = new ArrayList<SystemMetadata>();
        metaData.setSystems(list);
        assertEquals(list, metaData.getSystems());
    }

    @Test
    public void testPlatformSubscription() {
        PlatformSubscription sub = new PlatformSubscription();

        sub.setPlatform("platform");
        assertEquals("platform", sub.getPlatform());
        sub.setSelectAll(true);
        assertEquals(true, sub.isSelectAll());

        List<SystemSubscription> list = new ArrayList<SystemSubscription>();
        sub.setSystems(list);
        assertEquals(list, sub.getSystems());

        PlatformSubscription sub1 = new PlatformSubscription("pltfm");
    }

    @Test
    public void testSampleOut() {
        SampleOut so = new SampleOut();

        so.setDate(12345L);
        assertEquals(12345L, so.getDate());
        so.setPath("path");
        assertEquals("path", so.getPath());
    }

    @Test
    public void testSystemLevelMetrics() {
        SystemLevelMetrics slm = new SystemLevelMetrics();

        slm.setName("name");
        assertEquals("name", slm.getName());
        slm.setDq(12.3f);
        assertTrue(12.3f == slm.getDq());

        List<AssetLevelMetrics> list = new ArrayList<AssetLevelMetrics>();
        slm.setMetrics(list);
        assertEquals(list, slm.getMetrics());

        AssetLevelMetrics alm = new AssetLevelMetrics();
        slm.addAssetLevelMetrics(alm);
        assertEquals(1, slm.getMetrics().size());

        SystemLevelMetrics slm1 = new SystemLevelMetrics("name");
    }

    @Test
    public void testSystemLevelMetricsList() {
        SystemLevelMetricsList ml = new SystemLevelMetricsList();

        List<SystemLevelMetrics> list = new ArrayList<SystemLevelMetrics>();
        SystemLevelMetrics metric = new SystemLevelMetrics("system");
        ml.setLatestDQList(list);
        assertEquals(list, ml.getLatestDQList());

        List<AssetLevelMetrics> ametrics = new ArrayList<AssetLevelMetrics>();
        AssetLevelMetrics naMetric = new AssetLevelMetrics("almName", "metricType", 12.3f, 12345L, 3);
        List<AssetLevelMetricsDetail> details = new ArrayList<AssetLevelMetricsDetail>();
        details.add(new AssetLevelMetricsDetail(123L, 55.3f, 30.3f));
        naMetric.setDetails(details);
        ametrics.add(naMetric);
        metric.setMetrics(ametrics);
        list.add(metric);
        assertTrue(ml.containsAsset("system", "almName"));
        assertFalse(ml.containsAsset("system", "almName1"));

        assertEquals(metric, ml.getSystemLevelMetrics("system"));
        assertNull(ml.getSystemLevelMetrics("system1"));

        List<SystemLevelMetrics> slmList = ml.getListWithLatestNAssets(5, "system", null, null);
        assertNotNull(slmList);
        assertTrue(slmList.size() > 0);

        UserSubscription us = new UserSubscription("user");
        List<PlatformSubscription> psList = new ArrayList<PlatformSubscription>();
        PlatformSubscription ps = new PlatformSubscription("Apollo");
        List<SystemSubscription> ssList = new ArrayList<SystemSubscription>();
        SystemSubscription ss = new SystemSubscription("system");
        ss.setSelectAll(true);
        List<String> assets = new ArrayList<String>();
        assets.add("asset1");
        assets.add("asset2");
        ss.setDataassets(assets);
        ssList.add(ss);
        ps.setSystems(ssList);
        psList.add(ps);
        us.setSubscribes(psList);

        Map<String, String> tmp = new HashMap<String, String>();
        tmp.put("almName", "asset1");
        List<SystemLevelMetrics> slmList1 = ml.getListWithLatestNAssets(5, "system", us, tmp);
        assertNotNull(slmList1);
        assertTrue(slmList.size() > 0);

        AssetLevelMetrics alm = ml.getListWithSpecificAssetName("almName");
        assertNotNull(alm);
        assertEquals(naMetric, alm);
        AssetLevelMetrics alm1 = ml.getListWithSpecificAssetName("almName1");
        assertNull(alm1);

        alm = ml.getListWithSpecificAssetName("almName", 3);
        assertNotNull(alm);
        alm1 = ml.getListWithSpecificAssetName("almName1", 3);
        assertNull(alm1);

        Map<String, String> thresholds = new HashMap<String, String>();
        thresholds.put("almName", "20.5");
        thresholds.put("almName1", "50.2");
        ml.updateDQFail(thresholds);
        assertEquals(1, naMetric.getDqfail());

        List<SystemLevelMetrics> heatMapList = ml.getHeatMap(thresholds);
        assertNotNull(heatMapList);
        assertTrue(heatMapList.size() > 0);

        AssetLevelMetricsDetail detail = new AssetLevelMetricsDetail(12346L, 20.1f, 20.0f);
        detail.setBolling(new BollingerBandsEntity(300L, 100L, 200L));
        ml.upsertNewAssetExecute("almName", MetricType.Bollinger.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName", MetricType.Trend.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName", MetricType.MAD.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName", "", 12346L, 18.6f, "system", 0, true, detail);
        assertEquals(1, ml.getLatestDQList().size());
        ml.upsertNewAssetExecute("almName2", MetricType.Bollinger.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName3", MetricType.Trend.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName4", MetricType.MAD.toString(), 12346L, 18.6f, "system", 0, true, detail);
        ml.upsertNewAssetExecute("almName5", "", 12346L, 18.6f, "system", 0, true, detail);
        assertEquals(1, ml.getLatestDQList().size());
        ml.upsertNewAssetExecute("almName2", "metricType2", 12346L, 18.6f, "system2", 0, true, detail);
        assertEquals(2, ml.getLatestDQList().size());
    }

    @Test
    public void testSystemMetadata() {
        SystemMetadata sm = new SystemMetadata();

        sm.setId("id");
        assertEquals("id", sm.getId());
        sm.setName("name");
        assertEquals("name", sm.getName());

        List<DataAssetIndex> daiList = new ArrayList<DataAssetIndex>();
        sm.setAssets(daiList);
        assertEquals(daiList, sm.getAssets());
    }

    @Test
    public void testSystemSubscription() {
        SystemSubscription ss = new SystemSubscription();

        ss.setSystem("system");
        assertEquals("system", ss.getSystem());
        ss.setSelectAll(true);
        assertEquals(true, ss.isSelectAll());

        List<String> assets = new ArrayList<String>();
        ss.setDataassets(assets);
        assertEquals(assets, ss.getDataassets());
    }

    @Test
    public void testValidateHiveJobConfig() {
        ValidateHiveJobConfig config = new ValidateHiveJobConfig();

        config.setDataSet("dataset");
        assertEquals("dataset", config.getDataSet());

        List<ValidateHiveJobConfigLv1Detail> details = new ArrayList<ValidateHiveJobConfigLv1Detail>();
        List<ValidateHiveJobConfigLv2Detail> lv2Details = new ArrayList<ValidateHiveJobConfigLv2Detail>();
        lv2Details.add(new ValidateHiveJobConfigLv2Detail(2, 13.3));
        details.add(new ValidateHiveJobConfigLv1Detail(1, "col1", lv2Details));
        config.setValidityReq(details);
        assertEquals(details, config.getValidityReq());

        List<PartitionConfig> partitions = new ArrayList<PartitionConfig>();
        config.setTimePartitions(partitions);
        assertEquals(partitions, config.getTimePartitions());

        config.addColumnCalculation(1, "col1", 1);
        assertEquals(1, config.getValidityReq().size());

        // QUESTION: the result in lv2Detail is double, but getValue here returns long
        long result = config.getValue("col1", 2);
        assertEquals(13, result);
        result = config.getValue("col1", 3);
        assertEquals(Long.MIN_VALUE, result);

        ValidateHiveJobConfig config1 = new ValidateHiveJobConfig("dataset1");
        config1.addColumnCalculation(1, "col1", 1);
        assertEquals(1, config1.getValidityReq().size());
    }

    @Test
    public void testValidateHiveJobConfigLv1Detail() {
        ValidateHiveJobConfigLv1Detail detail = new ValidateHiveJobConfigLv1Detail();

        detail.setColId(1);
        assertEquals(1, detail.getColId());
        detail.setColName("name");
        assertEquals("name", detail.getColName());

        List<ValidateHiveJobConfigLv2Detail> lv2Details = new ArrayList<ValidateHiveJobConfigLv2Detail>();
        detail.setMetrics(lv2Details);
        assertEquals(lv2Details, detail.getMetrics());
    }

    @Test
    public void testValidateHiveJobConfigLv2Detail() {
        ValidateHiveJobConfigLv2Detail detail = new ValidateHiveJobConfigLv2Detail();

        detail.setName(1);
        assertEquals(1, detail.getName());
        detail.setResult(23.2);
        assertTrue(23.2 == detail.getResult());
    }
}
