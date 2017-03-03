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
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ebay.oss.griffin.common.ScheduleModelSeperator;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.DqSchedule;
import com.ebay.oss.griffin.domain.ModelStatus;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.repo.DqMetricsRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.repo.DqScheduleRepo;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelInput;
import com.mongodb.DBObject;

@Service
public class DqModelServiceImpl implements DqModelService {

	private static Logger logger = LoggerFactory.getLogger(DqModelServiceImpl.class);

	@Autowired
    private DqModelRepo dqModelRepo;

	@Autowired
    private DqScheduleRepo scheduleRepo;

	@Resource(name = "modelVoConverter")
	private Converter<DqModel, DqModelVo> converter;
	
	@Resource(name = "modelInputConverter")
	Converter<DqModel, ModelInput> modelInputConverter;

	@Autowired
    private DqMetricsRepo metricsRepo;
	
	@Override
	public List<DqModelVo> getAllModles() {
	    List<DqModelVo> allModels = new ArrayList<>();
		for(DqModel each : dqModelRepo.getAll()) {
            allModels.add( converter.voOf(each));
		}
		return allModels;
	}

	
    @Override
	public int deleteModel(String name) throws BarkDbOperationException {
		try {
			DqModel dqModel = dqModelRepo.findByName(name);

			// TODO need to mark related metrics as deleted, instead of real deletion
			// markMetricsDeleted(dqModel);

			dqModelRepo.delete(dqModel.get_id());

			if (dqModel.getModelType() == ModelType.ACCURACY) {
				scheduleRepo.deleteByModelList(name);
			} else if (dqModel.getModelType() == ModelType.VALIDITY) {
				DBObject currentSchedule = scheduleRepo.getValiditySchedule(dqModel.getAssetId());
				if (currentSchedule == null || currentSchedule.get("modelList") == null) {
					return -1;
				}

				String rawModelList = currentSchedule.get("modelList") .toString();
				String newModelList = "";
				if (rawModelList.contains(ScheduleModelSeperator.SEPERATOR)) {
					String[] rawModelArray = rawModelList.split(ScheduleModelSeperator.SPLIT_SEPERATOR);
					for (int i = 0; i < rawModelArray.length; i++) {
						if (!rawModelArray[i].equals(name))
							newModelList = newModelList + ScheduleModelSeperator.SEPERATOR + rawModelArray[i];
					}
					newModelList = newModelList.substring(ScheduleModelSeperator.SEPERATOR.length());
					currentSchedule.put("modelList", newModelList);
					scheduleRepo.updateModelType(currentSchedule, dqModel.getModelType());

				} else if (rawModelList.equals(name)) {
				    scheduleRepo.deleteByModelList(name);
				}
			}

			return 0;
		} catch (Exception e) {
			logger.warn(e.toString());
			throw new BarkDbOperationException("Failed to delete model of '"
					+ name + "'", e);
		}
	}

	// FIXME to be removed
	@Override
	public DqModel getGeneralModel(String name) {
		return dqModelRepo.findByName(name);
	}

	@Override
	public ModelInput getModelByName(String name) throws BarkDbOperationException {
		try {
			DqModel dqModel = dqModelRepo.findByName(name);
			return modelInputConverter.voOf(dqModel);
		} catch (Exception e) {
			logger.error(e.toString());
			throw new BarkDbOperationException("Failed to find model with name of '" + name + "'", e);
		}
	}

    @Override
	public void enableSchedule4Model(DqModel dqModel) {
		if(dqModel==null) return;

		dqModel.setStatus(ModelStatus.DEPLOYED);
		dqModelRepo.update(dqModel);

		if (dqModel.getModelType() == ModelType.ACCURACY
				|| dqModel.getModelType() == ModelType.VALIDITY) {
			DqSchedule schedule = new DqSchedule();
			schedule.set_id(scheduleRepo.getNextId());
			schedule.setScheduleType(dqModel.getSchedule());
			Date d = new Date(dqModel.getStarttime());
			d.setMinutes(0);
			d.setSeconds(0);
			schedule.setStarttime(d.getTime() / 1000 * 1000); // FIXME ????
			schedule.setStatus(0);
			schedule.setAssetId(dqModel.getAssetId());
			schedule.setJobType(dqModel.getModelType());

			String modellist = "";
			if (dqModel.getModelType() == ModelType.VALIDITY) {
				DBObject currentSchedule = scheduleRepo.getValiditySchedule(
								dqModel.getAssetId());
				if (currentSchedule != null) {
					if (currentSchedule.get("modelList") != null
							&& !currentSchedule.get("modelList").toString().equals("")) {
						modellist = currentSchedule.get("modelList")
								.toString();
						modellist = modellist
								+ ScheduleModelSeperator.SEPERATOR
								+ dqModel.getModelName();
					}
				}
				if (modellist.equals(""))
					modellist = dqModel.getModelName();
			} else
				modellist = dqModel.getModelName();
			schedule.setModelList(modellist);

			scheduleRepo.updateByModelType(schedule, dqModel.getModelType());
		}
	}

    @Resource(name = "modelCreatorChain")
    DqModelCreator modelCreator;
    
	@Override
	public DqModel newModel(ModelInput input) throws BarkDbOperationException {
	    return modelCreator.newModel(input);
	}

}