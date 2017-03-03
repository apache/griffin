package com.ebay.oss.griffin.domain;

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DataSchema;
import com.ebay.oss.griffin.domain.DqJob;
import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.DqSchedule;
import com.ebay.oss.griffin.domain.IdEntity;
import com.ebay.oss.griffin.domain.JobStatus;
import com.ebay.oss.griffin.domain.PartitionFormat;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.domain.UserSubscription;
import com.ebay.oss.griffin.vo.PlatformSubscription;
import com.ebay.oss.griffin.vo.SystemSubscription;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class DomainResourceTest {

    @Test
    public void testDataAsset() {
        DataAsset asset = new DataAsset();

        asset.setPlatform("platform");
        assertEquals("platform", asset.getPlatform());
        asset.setSystem("system");
        assertEquals("system", asset.getSystem());
        asset.setAssetName("name");
        assertEquals("name", asset.getAssetName());
        asset.setAssetType("type");
        assertEquals("type", asset.getAssetType());
        asset.setAssetHDFSPath("path");
        assertEquals("path", asset.getAssetHDFSPath());
        asset.setOwner("owner");
        assertEquals("owner", asset.getOwner());
        Date dt = new Date();
        asset.setTimestamp(dt);
        assertEquals(dt, asset.getTimestamp());

        List<DataSchema> schema = new ArrayList<DataSchema>();
        schema.add(new DataSchema("sname", "stype", "sdesc", "ssample"));
        asset.setSchema(schema);
        assertEquals(schema, asset.getSchema());

        List<PartitionFormat> partitions = new ArrayList<PartitionFormat>();
        partitions.add(new PartitionFormat("pname", "pformat"));
        asset.setPartitions(partitions);
        assertEquals(partitions, asset.getPartitions());

        assertEquals(asset.getColId("sname"), 0);
        assertEquals(asset.getColId("sname_unknown"), -1);
    }

    @Test
    public void testDataSchema() {
        DataSchema schema = new DataSchema();

        schema.setName("name");
        assertEquals("name", schema.getName());
        schema.setType("type");
        assertEquals("type", schema.getType());
        schema.setDesc("desc");
        assertEquals("desc", schema.getDesc());
        schema.setSample("sample");
        assertEquals("sample", schema.getSample());
    }

    @Test
    public void testDqMetricsValue() {
        DqMetricsValue dmv = new DqMetricsValue();

        dmv.setAssetId("id");
        assertEquals("id", dmv.getAssetId());
        dmv.setValue(3.56f);
        assertTrue(3.56f == dmv.getValue());
        dmv.setMetricName("name");
        assertEquals("name", dmv.getMetricName());
        dmv.setTimestamp(12345L);
        assertEquals(12345L, dmv.getTimestamp());
        dmv.set_id(12345L);
        assertEquals(new Long(12345L), dmv.get_id());

        DqMetricsValue dmv1 = new DqMetricsValue("name", 12345L, 20.3f);
        assertEquals(0, dmv1.compareTo(dmv));

        dmv1.setTimestamp(12346L);
        assertEquals(-1, dmv1.compareTo(dmv));
        dmv1.setTimestamp(12344L);
        assertEquals(1, dmv1.compareTo(dmv));
    }

    @Test
    public void testDqModel() {
        DqModel dm = new DqModel();

        dm.setModelId("id");
        assertEquals("id", dm.getModelId());
        dm.setModelName("name");
        assertEquals("name", dm.getModelName());
        dm.setModelType(2);
        assertEquals(2, dm.getModelType());
        dm.setModelDesc("desc");
        assertEquals("desc", dm.getModelDesc());
        dm.setAssetId(12345L);
        assertEquals(12345L, dm.getAssetId());
        dm.setThreshold(1.2f);
        assertTrue(1.2f == dm.getThreshold());
        dm.setNotificationEmail("abc@xxx.com");
        assertEquals("abc@xxx.com", dm.getNotificationEmail());
        dm.setOwner("owner");
        assertEquals("owner", dm.getOwner());
        dm.setStatus(1);
        assertEquals(1, dm.getStatus());
        dm.setModelContent("content");
        assertEquals("content", dm.getModelContent());
        dm.setTimestamp(12345L);
        assertEquals(12345L, dm.getTimestamp());
        dm.setSchedule(3);
        assertEquals(3, dm.getSchedule());
        dm.setSystem(1);
        assertEquals(1, dm.getSystem());
        dm.setAssetName("asset");
        assertEquals("asset", dm.getAssetName());
        dm.setReferenceModel("ref");
        assertEquals("ref", dm.getReferenceModel());
        dm.setStarttime(12345L);
        assertEquals(12345L, dm.getStarttime());

    }

    @Test
    public void testDqJob() {
        DqJob dj = new DqJob();

        dj.setId("id");
        assertEquals("id", dj.getId());
        dj.setModelList("list");
        assertEquals("list", dj.getModelList());
        dj.setContent("content");
        assertEquals("content", dj.getContent());
        dj.setStatus(3);
        assertEquals(3, dj.getStatus());
        dj.setStarttime(12345L);
        assertEquals(12345L, dj.getStarttime());
        dj.setEndtime(12345L);
        assertEquals(12345L, dj.getEndtime());
        dj.setValue(12345L);
        assertEquals(12345L, dj.getValue());
        dj.setJobType(2);
        assertEquals(2, dj.getJobType());
    }

    @Test
    public void testDqSchedule() {
        DqSchedule ds = new DqSchedule();

        ds.setStatus(1);
        assertEquals(1, ds.getStatus());
        ds.setStarttime(12345L);
        assertEquals(12345L, ds.getStarttime());
        ds.setContent("content");
        assertEquals("content", ds.getContent());
        ds.setScheduleType(2);
        assertEquals(2, ds.getScheduleType());
        ds.setModelList("list");
        assertEquals("list", ds.getModelList());
        ds.setAssetId(12345L);
        assertEquals(12345L, ds.getAssetId());
        ds.setJobType(3);
        assertEquals(3, ds.getJobType());

    }

    @Test
    public void testIdEntity() {
        IdEntity ide = new IdEntity();

        ide.set_id(12345L);
        assertEquals(new Long(12345L), ide.get_id());
    }

    @Test
    public void testJobStatus() {
        assertEquals(0, JobStatus.READY);
    }

    @Test
    public void testPartitionFormat() {
        PartitionFormat pf = new PartitionFormat();

        pf.setName("name");
        assertEquals("name", pf.getName());
        pf.setFormat("format");
        assertEquals("format", pf.getFormat());

        PartitionFormat pf1 = new PartitionFormat("name1", "format1");
        assertEquals("name1", pf1.getName());
        assertEquals("format1", pf1.getFormat());
    }

    @Test
    public void testSampleFilePathLKP() {
        SampleFilePathLKP sfp = new SampleFilePathLKP();

        sfp.setModelName("name");
        assertEquals("name", sfp.getModelName());
        sfp.setHdfsPath("path");
        assertEquals("path", sfp.getHdfsPath());
        sfp.setTimestamp(12345L);
        assertEquals(12345L, sfp.getTimestamp());
    }

    @Test
    public void testUserSubscription() {
        UserSubscription us = new UserSubscription();

        us.setId("id");
        assertEquals("id", us.getId());
        us.setNtaccount("count");
        assertEquals("count", us.getNtaccount());

        List<PlatformSubscription> psList = new ArrayList<PlatformSubscription>();
        PlatformSubscription ps = new PlatformSubscription("platform");
        ps.setSelectAll(true);
        List<SystemSubscription> ssList = new ArrayList<SystemSubscription>();
        SystemSubscription ss = new SystemSubscription("system");
        ss.setSelectAll(true);
        List<String> daList = new ArrayList<String>();
        daList.add("dataAsset");
        ss.setDataassets(daList);
        ssList.add(ss);
        ps.setSystems(ssList);
        psList.add(ps);
        us.setSubscribes(psList);
        assertEquals(psList, us.getSubscribes());

        assertTrue(us.isPlatformSelected("platform"));
        assertFalse(us.isPlatformSelected("platform1"));

        assertTrue(us.isSystemSelected("platform", "system"));
        assertFalse(us.isSystemSelected("platform", "system1"));

        assertTrue(us.isDataAssetSelected("platform", "system", "dataAsset"));
        assertFalse(us.isDataAssetSelected("platform", "system", "dataAsset1"));

        UserSubscription us1 = new UserSubscription("user");
    }
}
