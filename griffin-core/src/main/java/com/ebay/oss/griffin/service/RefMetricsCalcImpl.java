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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.ebay.oss.griffin.domain.AnomalyType;
import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.MetricType;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.ScheduleType;
import com.ebay.oss.griffin.domain.SystemType;
import com.ebay.oss.griffin.repo.DqMetricsRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.vo.AssetLevelMetricsDetail;
import com.ebay.oss.griffin.vo.BollingerBandsEntity;
import com.ebay.oss.griffin.vo.MADEntity;
import com.ebay.oss.griffin.vo.SystemLevelMetricsList;

@Component
public class RefMetricsCalcImpl implements RefMetrcsCalc {

    private static Logger logger = LoggerFactory.getLogger(RefMetricsCalcImpl.class);

	@Autowired
	DqModelService dqModelService;

	@Autowired
	DqModelRepo modelRepo;
	
	@Autowired
    DqMetricsRepo metricsRepo;

	Map<String, String> modelName_system;
	String getSystemType(String modelName) {
	    if(modelName_system==null) {
	        modelName_system = new HashMap<String, String>();

	        for(DqModel model : modelRepo.getAll()) {
	            modelName_system.put(model.getModelName(), SystemType.val(model.getModelType()));
	        }
	    }
	    return modelName_system.get(modelName);
    }

    @Override
    public void calc(SystemLevelMetricsList totalSystemLevelMetricsList) {
		Map<String, List<String>> references = getReferences();
		
		for(String modelName : references.keySet()) {
			List<String> refNames = references.get(modelName);

			for (String referencerName : refNames) {
			    calc(modelName, referencerName, totalSystemLevelMetricsList);
			}
		}
	}

	private void calc(String modelName, String referencerName, SystemLevelMetricsList totalSystemLevelMetricsList) {
	    logger.info("==============anmoni loop start==================" + referencerName + " " + modelName);
	    DqModel refModel = dqModelService.getGeneralModel(referencerName);
	    if (refModel == null) {
	        logger.warn("==============referencerModel is null================== "+referencerName);
//	        return false;
	        return;
	    }

	    DqModel sourceModel = dqModelService.getGeneralModel(modelName);
	    if (sourceModel == null) {
	        logger.warn("==============sourceModel is null================== "+sourceModel);
//	        return false;
	        return;
	    }
	    if (refModel.getModelType() != ModelType.ANOMALY) {
	        logger.warn("==============non-anomaly model founded================== "+referencerName);
//	        return true;
	        return;
	    }

//	    public static int trendLength = 20 * 24;
//	    public static int trendOffset = 24 * 7;

	    List<DqMetricsValue> metricList = metricsRepo.getByMetricsName(modelName);
	    Collections.sort(metricList);
	        
	    String content = refModel.getModelContent();
	    String[] contents = content.split("\\|");
	    int type = Integer.parseInt(contents[2]);

	    if (type == AnomalyType.HISTORY_TREND) {
	        calcHistoryRefModel(sourceModel, refModel,
	                                    metricList,totalSystemLevelMetricsList);
	    } else if (type == AnomalyType.BOLLINGER_BANDS) {
	        calcBollingerRefModel(modelName, referencerName, metricList
	                                            , totalSystemLevelMetricsList);
	    } else if (type == AnomalyType.MAD) {
	        calcMad(modelName, referencerName, refModel, metricList, totalSystemLevelMetricsList);
	    }
	    logger.info("==============anmoni loop end==================" + referencerName + " " + modelName);

    }
    protected void calcHistoryRefModel(DqModel sourceModel, DqModel refModel
                                            , List<DqMetricsValue> metricList
                                            , SystemLevelMetricsList totalSystemLevelMetricsList) {
        int trendLength, trendOffset;
        if (sourceModel.getSchedule() == ScheduleType.DAILY) {
            trendLength = 20;
            trendOffset = 7;
        } else {
            trendLength = 20 * 24;
            trendOffset = 7 * 24;
        }

        if (metricList.size() <= trendLength + trendOffset) {
            return;
//            return false;
        }

        String modelName = sourceModel.getModelName();
        String referencerName = refModel.getModelName();
        logger.info("==============trend start=================="
                        + referencerName
                        + " "
                        + modelName
                        + " "
                        + trendLength + " " + trendOffset);

        int dqfail = 0;
        float threadshold = refModel.getThreshold();
        if (metricList.get(0).getValue() / metricList.get(trendOffset) .getValue() >= 1 + threadshold
                        || metricList.get(0).getValue() / metricList.get(trendOffset) .getValue() <= 1 - threadshold) {
            dqfail = 1;
        }

        for (int i = 0; i <= trendLength; i++) {
            DqMetricsValue tempDQMetricsValue = metricList.get(i);
            float lastValue = metricList.get( i + trendOffset).getValue();
            totalSystemLevelMetricsList.upsertNewAssetExecute(
                            referencerName,
                            MetricType.Trend.toString(),
                            tempDQMetricsValue.getTimestamp(),
                            tempDQMetricsValue.getValue()
                            , getSystemType(tempDQMetricsValue.getMetricName())
                            , dqfail,
                            true, new AssetLevelMetricsDetail(lastValue));
        }

        logger.info("==============trend end==================");
    }


    protected void calcMad(String modelName, String referencerName, DqModel refModel,
                    List<DqMetricsValue> metricList,
                    SystemLevelMetricsList totalSystemLevelMetricsList) {
        logger.info("==============MAD start==================" + referencerName + " " + modelName);
        Collections.reverse(metricList);
        List<String> sourceValues = new ArrayList<String>();
        for (int i = 0; i < metricList.size(); i++) {
            sourceValues.add((long) metricList.get(i).getValue() + "");
        }
        List<MADEntity> madList = createMad(sourceValues);

        logger.info("==============MAD size : "+madList.size() +" metrics size:"+metricList.size());
        if (metricList.size() > 0 && madList.size() > 0) {
            int dqfail = 0;
            if (metricList.get( metricList.size() - 1).getValue() 
                            < madList.get(madList.size() - 1).getLower()) {
                dqfail = 1;
            }

            int offset = metricList.size() - madList.size();
            for (int i = offset; i < metricList.size(); i++) {
                DqMetricsValue tempDQMetricsValue = metricList.get(i);

                MADEntity mad = madList.get( i - offset).clone();
                AssetLevelMetricsDetail detail = new AssetLevelMetricsDetail( mad);
                totalSystemLevelMetricsList.upsertNewAssetExecute(
                                                referencerName,
                                                MetricType.MAD.toString(),
                                                tempDQMetricsValue.getTimestamp(),
                                                tempDQMetricsValue.getValue(),
                                                getSystemType(tempDQMetricsValue.getMetricName()),
                                                dqfail,
                                                true,
                                                detail
                                                );
            }
        }
        logger.info("==============MAD end==================");
    }

    protected void calcBollingerRefModel(String modelName, String referencerName,
                    List<DqMetricsValue> metricList,
                    SystemLevelMetricsList totalSystemLevelMetricsList) {
        logger.info("==============Bollinger start=================="
                        + referencerName + " " + modelName);
        Collections.reverse(metricList);
        List<String> sourceValues = new ArrayList<String>();
        for (int i = 0; i < metricList.size(); i++) {
            sourceValues.add((long) metricList.get(i)
                            .getValue() + "");
        }

        List<BollingerBandsEntity> bollingers = bollingerBand(sourceValues);

        logger.info("==============Bollinger size : "+bollingers.size() +" metrics size:"+metricList.size());
        if (metricList.size() > 0 && bollingers.size() > 0) {
            int dqfail = 0;
            if (metricList.get( metricList.size() - 1).getValue() 
                            < bollingers.get(bollingers.size() - 1).getLower()) {
                dqfail = 1;
            }

            int offset = metricList.size()- bollingers.size();
            for (int i = offset; i < metricList.size(); i++) {
                DqMetricsValue tempDQMetricsValue = metricList.get(i);

                BollingerBandsEntity bollinger = bollingers.get(i - offset).clone();
                AssetLevelMetricsDetail detail = new AssetLevelMetricsDetail(bollinger);
                totalSystemLevelMetricsList.upsertNewAssetExecute(
                                referencerName,
                                MetricType.Bollinger.toString(),
                                tempDQMetricsValue.getTimestamp(),
                                tempDQMetricsValue.getValue(),
                                getSystemType(tempDQMetricsValue.getMetricName()),
                                dqfail,
                                true,
                                detail
                                );
            }
        }
        logger.info("==============Bollinger end=================="
                        + referencerName + " " + modelName);
    }

    private List<String> parseRefNames(String reference) {
	    List<String> refNames = new ArrayList<String>();
	    if (reference.indexOf(",") == -1) {
	        refNames.add(reference);
	    } else {
	        refNames = Arrays.asList(reference.split(","));
	    }
	    return refNames;
    }

	/** <modelName, [refModelName]> */
    Map<String, List<String>> getReferences() {
	    Map<String, List<String>> map = new HashMap<>();
	    for(DqModel each : modelRepo.getAll()) {
	        String modelName = each.getModelName();
	        String references = each.getReferenceModel();
	        if(!map.containsKey(each.getModelName())) {
	            map.put(modelName, new ArrayList<String>());
	        }
	        if(StringUtils.hasText(references)) {
	            for(String ref : parseRefNames(references)) {
	                map.get(modelName).add(ref.trim());
	            }
	        }
	    }
	    return map;
    }

	List<BollingerBandsEntity> bollingerBand(List<String> list) {
		List<BollingerBandsEntity> result = new ArrayList<BollingerBandsEntity>();
		int preparePointNumber = 30;
		float up_coff = 1.8f;
		float down_coff = 1.8f;
		for (int i = preparePointNumber; i < list.size(); i++) {
			long total = 0;
			for (int j = i - preparePointNumber; j < i; j++) {
				long rawNumber = Long.parseLong(list.get(j));
				total = total + rawNumber;
			}
			long mean = total / preparePointNumber;
			long meantotal = 0;
			for (int j = i - preparePointNumber; j < i; j++) {
				long rawNumber = Integer.parseInt(list.get(j));
				long rawDiff = rawNumber - mean;
				meantotal += rawDiff * rawDiff;
			}
			long mad = (long) Math.sqrt(meantotal / preparePointNumber);
			long upper = (long) (mean + mad * up_coff);
			long lower = (long) (mean - mad * down_coff);
			// .out.println( list.get(i)+"\t"+upper +"\t"+lower);
			result.add(new BollingerBandsEntity(upper, lower, mean));
		}
		logger.info("bollingerband done");
		return result;
	}

    List<MADEntity> createMad(List<String> list) {
		List<MADEntity> result = new ArrayList<MADEntity>();
		int preparePointNumber = 15;
		float up_coff = 2.3f;
		float down_coff = 2.3f;
		for (int i = preparePointNumber; i < list.size(); i++) {
			long total = 0;
			for (int j = i - preparePointNumber; j < i; j++) {
				long rawNumber = Long.parseLong(list.get(j));
				total = total + rawNumber;
			}
			long mean = total / preparePointNumber;
			long meantotal = 0;
			for (int j = i - preparePointNumber; j < i; j++) {
				long rawNumber = Integer.parseInt(list.get(j));
				long rawDiff = rawNumber - mean;
				if (rawDiff >= 0)
					meantotal = meantotal + rawDiff;
				else
					meantotal = meantotal - rawDiff;
			}
			long mad = meantotal / preparePointNumber;
			long upper = (long) (mean + mad * up_coff);
			long lower = (long) (mean - mad * down_coff);

			result.add(new MADEntity(upper, lower));
		}
		logger.info("mad done");
		return result;
	}

}
