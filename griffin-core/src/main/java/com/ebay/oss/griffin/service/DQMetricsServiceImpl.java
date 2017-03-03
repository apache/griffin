/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.ebay.oss.griffin.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ebay.oss.griffin.common.Pair;
import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.ebay.oss.griffin.domain.SystemType;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.repo.DataAssetRepo;
import com.ebay.oss.griffin.repo.DqMetricsRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.repo.SampleFilePathRepo;
import com.ebay.oss.griffin.vo.AssetLevelMetrics;
import com.ebay.oss.griffin.vo.DQHealthStats;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.OverViewStatistics;
import com.ebay.oss.griffin.vo.SampleOut;
import com.ebay.oss.griffin.vo.SystemLevelMetrics;
import com.ebay.oss.griffin.vo.SystemLevelMetricsList;
import com.mongodb.DBObject;

@Service("dqmetrics")
public class DQMetricsServiceImpl implements DQMetricsService {

    private static Logger logger = LoggerFactory.getLogger(DQMetricsServiceImpl.class);

    @Autowired
    private DqModelService dqModelService;

    @Autowired
    private SubscribeService subscribeService;

    @Autowired
    private DqMetricsRepo metricsRepo;

    @Autowired
    private DqModelRepo modelRepo;

    @Autowired
    private DataAssetRepo dataAssetRepo;

    @Autowired
    private SampleFilePathRepo missedFileRepo;

    @Autowired
    private RefMetrcsCalc refMetricCalc;
    
    public static List<DqMetricsValue> cacheValues = new ArrayList<DqMetricsValue>();;

    public static SystemLevelMetricsList totalSystemLevelMetricsList;

    public void insertMetadata(DqMetricsValue metrics) {

        List<Pair> queryList = new ArrayList<Pair>();
        queryList.add(new Pair("metricName", metrics.getMetricName()));
        // queryList.add(new KeyValue("metricType", dq.getMetricType()));
        // queryList.add(new KeyValue("assetId", dq.getAssetId()));
        queryList.add(new Pair("timestamp", metrics.getTimestamp()));

        List<Pair> updateValues = new ArrayList<Pair>();
        updateValues.add(new Pair("value", metrics.getValue()));

        DBObject item = metricsRepo.getByCondition(queryList);

        try {
            if (item == null) {
                long seq = metricsRepo.getNextId();
                logger.info("log: new record inserted" + seq);
                metrics.set_id(seq);
                metricsRepo.save(metrics);
            } else {
                logger.info("log: updated record");
                metricsRepo.update(metrics, item);
            }
        }catch(Exception e) {
            throw new BarkDbOperationException("Failed to save metrics value!", e);
        }

    }

    public DqMetricsValue getLatestlMetricsbyId(String assetId) {
        return metricsRepo.getLatestByAssetId(assetId);
    }

    void refreshAllDQMetricsValuesinCache() {
        refreshModelSystemCache();

        cacheValues.clear();
        for (DqMetricsValue each : metricsRepo.getAll()) {
            cacheValues.add(each);
        }
    }
    Map<String, String> modelSystem = new HashMap<String, String>();
    void refreshModelSystemCache() {
        for (DqModel model : modelRepo.getAll()) {
            modelSystem.put(model.getModelName(), SystemType.val(model.getSystem()));
        }
    }

    public synchronized void updateLatestDQList() {
        try {
            logger.info("==============updating all latest dq metrics==================");
            refreshAllDQMetricsValuesinCache();

            totalSystemLevelMetricsList = new SystemLevelMetricsList();
            for (DqMetricsValue temp : cacheValues) {
                // totalSystemLevelMetricsList.upsertNewAsset(temp, assetSystem,
                // 1);
                totalSystemLevelMetricsList.upsertNewAssetExecute(
                        temp.getMetricName(), "", temp.getTimestamp(),
                        temp.getValue(), modelSystem.get(temp.getMetricName()),
                        0, true, null);
            }

            totalSystemLevelMetricsList.updateDQFail(getThresholds());
            refMetricCalc.calc(totalSystemLevelMetricsList);

            logger.info("==============update all latest dq metrics done==================");
        } catch (Exception e) {
            logger.error("{}", e);
        }
    }

    Map<String, String> getThresholds() {
        Map<String, String> thresHolds = new HashMap<>();
        for(DqModel each : modelRepo.getAll()) {
            thresHolds.put(each.getModelName(), "" + each.getThreshold());
        }
        return thresHolds;
    }

    List<SystemLevelMetrics> addAssetNames(
            List<SystemLevelMetrics> result) {
        List<DqModelVo> models = dqModelService.getAllModles();
        Map<String, String> modelMap = new HashMap<String, String>();

        for (DqModelVo model : models) {
            modelMap.put(
                    model.getName(),
                    model.getAssetName() == null ? "unknow" : model
                            .getAssetName());
        }

        for (SystemLevelMetrics sys : result) {
            List<AssetLevelMetrics> assetList = sys.getMetrics();
            if (assetList != null && assetList.size() > 0) {
                for (AssetLevelMetrics metrics : assetList) {
                    metrics.setAssetName(modelMap.get(metrics.getName()));
                }
            }
        }

        return result;
    }

    Map<String, String> getAssetMap() {
        Map<String, String> modelMap = new HashMap<String, String>();

        for (DqModelVo model : dqModelService.getAllModles()) {
            modelMap.put( model.getName(),
                            model.getAssetName() == null ? "unknow" : model.getAssetName());
        }

        return modelMap;
    }

    @Override
    public List<SystemLevelMetrics> briefMetrics(String system) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return totalSystemLevelMetricsList.getListWithLatestNAssets(24, system, null, null);
    }

    @Override
    public List<SystemLevelMetrics> heatMap() {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return totalSystemLevelMetricsList.getHeatMap(getThresholds());
    }

    @Override
    public List<SystemLevelMetrics> dashboard(String system) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return addAssetNames(totalSystemLevelMetricsList
                .getListWithLatestNAssets(30, system, null, null));
    }

    @Override
    public List<SystemLevelMetrics> mydashboard(String user) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return addAssetNames(totalSystemLevelMetricsList
                .getListWithLatestNAssets(30, "all",
                        subscribeService.getSubscribe(user), getAssetMap()));
    }

    @Override
    public AssetLevelMetrics oneDataCompleteDashboard(String name) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return totalSystemLevelMetricsList.getListWithSpecificAssetName(name);
    }

    @Override
    public AssetLevelMetrics oneDataBriefDashboard(String name) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return totalSystemLevelMetricsList.getListWithSpecificAssetName(name, 30);
    }

    public OverViewStatistics getOverViewStats() {

        OverViewStatistics os = new OverViewStatistics();

        os.setAssets(dataAssetRepo.getAll().size());
        os.setMetrics(modelRepo.getAll().size());

        DQHealthStats health = new DQHealthStats();

        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();

        List<SystemLevelMetrics> allMetrics = totalSystemLevelMetricsList
                .getLatestDQList();

        int healthCnt = 0;
        int invalidCnt = 0;

        for (SystemLevelMetrics metricS : allMetrics) {

            List<AssetLevelMetrics> metricsA = metricS.getMetrics();

            for (AssetLevelMetrics m : metricsA) {
                if (m.getDqfail() == 0) {
                    healthCnt++;
                } else {
                    invalidCnt++;
                }
            }
        }

        health.setHealth(healthCnt);
        health.setInvalid(invalidCnt);

        health.setWarn(0);
        os.setStatus(health);

        return os;

    }

    /**
     * Get the metrics for 24 hours
     */
    @Override
    public AssetLevelMetrics metricsForReport(String name) {
        if (totalSystemLevelMetricsList == null)
            updateLatestDQList();
        return totalSystemLevelMetricsList.getListWithSpecificAssetName(name, 24);
    }

    @Override
    public List<SampleOut> listSampleFile(String modelName) {

        List<SampleOut> samples = new ArrayList<SampleOut>();

        List<DBObject> dbos = missedFileRepo.findByModelName(modelName);

        for (DBObject dbo : dbos) {

            SampleOut so = new SampleOut();

            so.setDate(Long.parseLong(dbo.get("timestamp").toString()));
            so.setPath(dbo.get("hdfsPath").toString());

            samples.add(so);
        }

        return samples;

    }

    @Override
    public void insertSampleFilePath(SampleFilePathLKP samplePath) {
        SampleFilePathLKP entity = new SampleFilePathLKP();

        entity.set_id(missedFileRepo.getNextId());
        entity.setModelName(samplePath.getModelName());
        entity.setTimestamp(samplePath.getTimestamp());
        entity.setHdfsPath(samplePath.getHdfsPath());

        missedFileRepo.save(entity);

    }

}
