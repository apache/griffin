package com.ebay.oss.griffin.service;

import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.vo.ModelBasicInputNew;
import com.ebay.oss.griffin.vo.ModelExtraInputNew;
import com.ebay.oss.griffin.vo.ModelInput;

@Component("modelInputConverter")
public class ModelInputConverter implements Converter<DqModel, ModelInput> {

    @Override
    public ModelInput voOf(DqModel dqModel) {
		if(dqModel == null){
			return null;
		}

		// Object result = sourceObject;
		int modelType = dqModel.getModelType();
		ModelInput result = new ModelInput();
		result.setBasic(getViewModelForFront(dqModel));

		if (modelType == ModelType.ACCURACY) {
			result.parseFromString(dqModel.getModelContent());
		} else if (modelType == ModelType.VALIDITY) {

			ModelExtraInputNew extra = result.getExtra();
			String content = dqModel.getModelContent();
			String[] contents = content.split("\\|");
			extra.setSrcDb(contents[0]);
			extra.setSrcDataSet(contents[1]);
			extra.setVaType(Integer.parseInt(contents[2]));
			extra.setColumn(contents[3]);


		} else if (modelType == ModelType.ANOMALY) {

			ModelExtraInputNew extra = result.getExtra();
			String content = dqModel.getModelContent();
			String[] contents = content.split("\\|");
			extra.setSrcDb(contents[0]);
			extra.setSrcDataSet(contents[1]);
			int type = Integer.parseInt(contents[2]);
			extra.setAnType(type);

		} else if (modelType == ModelType.PUBLISH) {

			result.getExtra().setPublishUrl(dqModel.getModelContent());
		}

		return result;
	}

	ModelBasicInputNew getViewModelForFront(DqModel sourceObject) {
		ModelBasicInputNew basic = new ModelBasicInputNew();
		basic.setDesc(sourceObject.getModelDesc());
		basic.setName(sourceObject.getModelName());
		basic.setDataaset(sourceObject.getAssetName());
		basic.setDataasetId(sourceObject.getAssetId());
		basic.setStatus(sourceObject.getStatus());
		basic.setType(sourceObject.getModelType());
		basic.setScheduleType(sourceObject.getSchedule());
		basic.setSystem(sourceObject.getSystem());
		basic.setEmail(sourceObject.getNotificationEmail());
		basic.setOwner(sourceObject.getOwner());
		basic.setThreshold(sourceObject.getThreshold());

		return basic;
	}

    @Override
    public DqModel entityOf(ModelInput vo) {
        // TODO Auto-generated method stub
        throw new RuntimeException("not implemented yet...");
    }


}
