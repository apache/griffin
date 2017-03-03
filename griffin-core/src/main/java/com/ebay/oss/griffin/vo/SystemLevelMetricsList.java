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
package com.ebay.oss.griffin.vo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.oss.griffin.domain.MetricType;
import com.ebay.oss.griffin.domain.UserSubscription;

public class SystemLevelMetricsList {

    private static Logger logger = LoggerFactory.getLogger(SystemLevelMetricsList.class);

    public List<SystemLevelMetrics> latestDQList = new ArrayList<SystemLevelMetrics>();

    public String limitPlatform = "Apollo";

    boolean containsAsset(String system, String name) {
        for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList) {
            if (tempSystemLevelMetrics.getName().equals(system)) {
                for (AssetLevelMetrics tempAssetLevelMetrics : tempSystemLevelMetrics.getMetrics()) {
                    if (tempAssetLevelMetrics.getName().equals(name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public SystemLevelMetrics getSystemLevelMetrics(String system) {
        for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList) {
            if (tempSystemLevelMetrics.getName().equals(system)) {
                return tempSystemLevelMetrics;
            }
        }
        return null;
    }


    public List<SystemLevelMetrics> getListWithLatestNAssets(int N, String system,
                    UserSubscription subscribe, Map<String, String> assetMap) {

        List<SystemLevelMetrics> result = new ArrayList<SystemLevelMetrics>();
        try {

            for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList) {
                if (tempSystemLevelMetrics.getName().equals(system) || system.equals("all")) {

                    SystemLevelMetrics tempSystemLevelMetrics1 =
                                    new SystemLevelMetrics(tempSystemLevelMetrics.getName());
                    tempSystemLevelMetrics1.setDq(tempSystemLevelMetrics.getDq());
                    List<AssetLevelMetrics> metrics = tempSystemLevelMetrics.getMetrics();

                    boolean isCurrentSystemSelected;
                    if (subscribe != null)
                        isCurrentSystemSelected =
                                        subscribe.isSystemSelected(limitPlatform,
                                                        tempSystemLevelMetrics.getName());
                    else
                        isCurrentSystemSelected = true;

                    for (AssetLevelMetrics tempAssetLevelMetrics : metrics)
                    {
                        boolean isCurrentDataassetSelected;
                        if (subscribe != null)
                            isCurrentDataassetSelected =
                                            subscribe.isDataAssetSelected(limitPlatform,
                                                            tempSystemLevelMetrics.getName(),
                                                            assetMap.get(tempAssetLevelMetrics
                                                                            .getName()));
                        else
                            isCurrentDataassetSelected = true;

                        if (isCurrentSystemSelected || isCurrentDataassetSelected)
                        {
                            AssetLevelMetrics tempAssetLevelMetrics1 =
                                            new AssetLevelMetrics(tempAssetLevelMetrics.getName(),
                                                            tempAssetLevelMetrics.getMetricType(),
                                                            tempAssetLevelMetrics.getDq(),
                                                            tempAssetLevelMetrics.getTimestamp(),
                                                            tempAssetLevelMetrics.getDqfail());
                            List<AssetLevelMetricsDetail> otherdetails =
                                            tempAssetLevelMetrics.getDetails();
                            List<AssetLevelMetricsDetail> tempdetails =
                                            new ArrayList<AssetLevelMetricsDetail>();
                            if (otherdetails != null)
                            {
                                Collections.sort(otherdetails);
                                for (int i = 0; i < otherdetails.size() && i < N; i++)
                                {
                                    tempdetails.add(otherdetails.get(i));
                                }
                            }

                            tempAssetLevelMetrics1.setDetails(tempdetails);

                            tempSystemLevelMetrics1.addAssetLevelMetrics(tempAssetLevelMetrics1);
                        }
                    }

                    if (tempSystemLevelMetrics1.getMetrics().size() > 0)
                        result.add(tempSystemLevelMetrics1);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e);
        }
        return result;
    }

    public AssetLevelMetrics getListWithSpecificAssetName(String name)
    {
        for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList)
        {
            SystemLevelMetrics tempSystemLevelMetrics1 =
                            new SystemLevelMetrics(tempSystemLevelMetrics.getName());
            tempSystemLevelMetrics1.setDq(tempSystemLevelMetrics.getDq());
            for (AssetLevelMetrics tempAssetLevelMetrics : tempSystemLevelMetrics.getMetrics())
            {
                if (tempAssetLevelMetrics.getName().equals(name))
                {
                    return tempAssetLevelMetrics;
                }
            }
        }

        return null;
    }

    public AssetLevelMetrics getListWithSpecificAssetName(String name, int count)
    {
        for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList)
        {
            SystemLevelMetrics tempSystemLevelMetrics1 =
                            new SystemLevelMetrics(tempSystemLevelMetrics.getName());
            tempSystemLevelMetrics1.setDq(tempSystemLevelMetrics.getDq());
            for (AssetLevelMetrics tempAssetLevelMetrics : tempSystemLevelMetrics.getMetrics())
            {
                if (tempAssetLevelMetrics.getName().equals(name))
                {
                    return new AssetLevelMetrics(tempAssetLevelMetrics, count);
                }
            }
        }

        return null;
    }

    public void updateDQFail(Map<String, String> thresholds)
    {
        for (SystemLevelMetrics tempSystemLevelMetrics : latestDQList)
        {
            for (AssetLevelMetrics tempAssetLevelMetrics : tempSystemLevelMetrics.getMetrics())
            {
                if (thresholds.containsKey(tempAssetLevelMetrics.getName()))
                {
                    if (tempAssetLevelMetrics.getDq() < Float.parseFloat(thresholds
                                    .get(tempAssetLevelMetrics.getName())))
                    {
                        tempAssetLevelMetrics.setDqfail(1);
                    }
                }
            }
        }
    }


    public List<SystemLevelMetrics> getHeatMap(Map<String, String> thresholds) {
        List<SystemLevelMetrics> result = new ArrayList<SystemLevelMetrics>();

        SystemLevelMetricsList latestSysMetricsList = new SystemLevelMetricsList();
        latestSysMetricsList.setLatestDQList(latestDQList);
        SystemLevelMetricsList resultSysMetricsList = new SystemLevelMetricsList();

        for (SystemLevelMetrics tempSystemMetrics : latestDQList) {
            int size = 0;
            for (AssetLevelMetrics tempAssetLevelMetrics : tempSystemMetrics.getMetrics()) {
                if (thresholds.containsKey(tempAssetLevelMetrics.getName())) {
                    if (tempAssetLevelMetrics.getDq() < Float.parseFloat(thresholds
                                    .get(tempAssetLevelMetrics.getName()))) {
                        tempAssetLevelMetrics.setDqfail(1);
                        resultSysMetricsList.upsertNewAssetExecute(
                                        tempAssetLevelMetrics.getName(),
                                        tempAssetLevelMetrics.getMetricType(),
                                        tempAssetLevelMetrics.getTimestamp(),
                                        tempAssetLevelMetrics.getDq(),
                                        tempSystemMetrics.getName(),
                                        tempAssetLevelMetrics.getDqfail(), false, null);

                        size++;
                    }
                }
            }
            if (size == 0) {
                SystemLevelMetrics sysMetric = new SystemLevelMetrics(tempSystemMetrics.getName());
                resultSysMetricsList.getLatestDQList().add(sysMetric);
            }
        }

        result = resultSysMetricsList.getLatestDQList();
        for (SystemLevelMetrics tempSystemLevelMetrics : result) {
            int size = tempSystemLevelMetrics.getMetrics().size();
            String system = tempSystemLevelMetrics.getName();
            if (size >= 8) {
                continue;
            }

            SystemLevelMetrics latestSystLvlMetrics = latestSysMetricsList.getSystemLevelMetrics(tempSystemLevelMetrics
                                            .getName());
            for (AssetLevelMetrics latestAssMetrics : latestSystLvlMetrics .getMetrics()) {
                if (!resultSysMetricsList.containsAsset(system, latestAssMetrics.getName())) {
                    resultSysMetricsList.upsertNewAssetExecute(
                                    latestAssMetrics.getName(),
                                    latestAssMetrics.getMetricType(),
                                    latestAssMetrics.getTimestamp(),
                                    latestAssMetrics.getDq(),
                                    system,
                                    latestAssMetrics.getDqfail(),
                                    false, null);
                    size++;
                    if (size >= 8)
                        break;
                }
            }
        }

        return result;
    }

    public void upsertNewAssetExecute(String metricName, String metricType, long metricTs,
                    float metricValue, String currentSystem, 
                    int dqfail, boolean needdetail,
                    AssetLevelMetricsDetail otherAttributes) {
        boolean systemFound = false;
        if (currentSystem == null) {
            currentSystem = "unknown";
        }
        try {
            for (SystemLevelMetrics tmpSysMetrics : latestDQList) {
                // find the system item
                if (!tmpSysMetrics.getName().equals(currentSystem)) {
                    continue;
                }
                systemFound = true;

                List<AssetLevelMetrics> tmpAssMetricsList = tmpSysMetrics.getMetrics();
                boolean metricFound = false;
                for (int k = 0; k < tmpAssMetricsList.size(); k++) {
                    AssetLevelMetrics tempAssetLevelMetrics = tmpAssMetricsList.get(k);
                    // find the metric
                    if (!tempAssetLevelMetrics.getName().equals(metricName)) {
                        continue;
                    }
                    metricFound = true;

                    if (tempAssetLevelMetrics.getTimestamp() - (metricTs) < 0) {
                        tempAssetLevelMetrics.setTimestamp(metricTs);
                        tempAssetLevelMetrics.setDq(metricValue);
                        tempAssetLevelMetrics.setDqfail(dqfail);
                        tmpAssMetricsList.set(k, tempAssetLevelMetrics); // FIXME why???
                    }
                    if (!needdetail) {
                        continue;
                    }

                    if (metricType.equals(MetricType.Bollinger.toString())) {
                        AssetLevelMetricsDetail detail =  new AssetLevelMetricsDetail(
                                        metricTs,
                                        metricValue, otherAttributes.getBolling().clone()
                                        );
                        tempAssetLevelMetrics.addAssetLevelMetricsDetail(detail);
                    } else if (metricType.equals(MetricType.Trend.toString())) {
                         AssetLevelMetricsDetail detail = new AssetLevelMetricsDetail(
                                                            metricTs,
                                                            metricValue,
                                                            otherAttributes.getComparisionValue());
                        tempAssetLevelMetrics.addAssetLevelMetricsDetail(detail);
                    } else if (metricType.equals(MetricType.MAD.toString())) {
                        tempAssetLevelMetrics
                        .addAssetLevelMetricsDetail(new AssetLevelMetricsDetail(
                                        metricTs, metricValue, otherAttributes
                                        .getMAD()));
                    } else {
                        tempAssetLevelMetrics
                        .addAssetLevelMetricsDetail(new AssetLevelMetricsDetail(
                                        metricTs, metricValue));
                    }
                }

                // didn't find the metric, create one 
                if (!metricFound ) {
                    AssetLevelMetrics newTempAssetLevelMetrics =
                                    new AssetLevelMetrics(metricName, metricType, metricValue,
                                                    metricTs, dqfail);
                    if (needdetail ) {
                        if (metricType.equals(MetricType.Bollinger.toString())) {
                            AssetLevelMetricsDetail detail =  
                                            new AssetLevelMetricsDetail(
                                            metricTs,
                                            metricValue,
                                            otherAttributes.getBolling().clone());
                            newTempAssetLevelMetrics .addAssetLevelMetricsDetail(detail);
                        } else if (metricType.equals(MetricType.Trend.toString())) {
                            newTempAssetLevelMetrics
                            .addAssetLevelMetricsDetail(new AssetLevelMetricsDetail(
                                            metricTs,
                                            metricValue,
                                            otherAttributes.getComparisionValue()));
                        } else if (metricType.equals(MetricType.MAD.toString())) {
                            newTempAssetLevelMetrics
                            .addAssetLevelMetricsDetail(new AssetLevelMetricsDetail(
                                            metricTs, metricValue, otherAttributes
                                            .getMAD()));
                        } else {
                            newTempAssetLevelMetrics
                            .addAssetLevelMetricsDetail(new AssetLevelMetricsDetail(
                                            metricTs, metricValue));
                        }
                    }
                    tmpAssMetricsList.add(newTempAssetLevelMetrics);
                }
            }
        } catch (Exception e) {
            logger.error("{}", e);
        }

        // can't find the system
        if (!systemFound ) {
            SystemLevelMetrics newSystemLevelMetrics = new SystemLevelMetrics(currentSystem);
            newSystemLevelMetrics.addAssetLevelMetrics(new AssetLevelMetrics(metricName,
                            metricType, metricValue, metricTs, dqfail));
            if (needdetail ) {
                List<AssetLevelMetricsDetail> tempDetailList =
                                new ArrayList<AssetLevelMetricsDetail>();
                tempDetailList.add(new AssetLevelMetricsDetail(metricTs, metricValue));
                newSystemLevelMetrics.getMetrics().get(0).setDetails(tempDetailList);
            }
            latestDQList.add(newSystemLevelMetrics);
        }
    }


    public List<SystemLevelMetrics> getLatestDQList() {
        return latestDQList;
    }

    public void setLatestDQList(List<SystemLevelMetrics> latestDQList) {
        this.latestDQList = latestDQList;
    }


}
