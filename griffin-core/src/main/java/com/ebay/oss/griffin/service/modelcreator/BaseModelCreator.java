package com.ebay.oss.griffin.service.modelcreator;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DqJob;
import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.domain.ModelType;
import com.ebay.oss.griffin.domain.ScheduleType;
import com.ebay.oss.griffin.domain.ValidityType;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.repo.DataAssetRepo;
import com.ebay.oss.griffin.repo.DqJobRepo;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.service.DqModelCreator;
import com.ebay.oss.griffin.vo.ModelBasicInputNew;
import com.ebay.oss.griffin.vo.ModelExtraInputNew;
import com.ebay.oss.griffin.vo.ModelInput;

public abstract class BaseModelCreator implements DqModelCreator {

    private static Logger logger = LoggerFactory.getLogger(BaseModelCreator.class);

    @Autowired
	DqModelRepo dqModelRepo;

    @Autowired
    DataAssetRepo dataAssetRepo;

	@Autowired
    private DqJobRepo jobRepo;

    boolean hasModelWithName(String name) {
		return null != dqModelRepo.findByName(name);
	}
	@Override
	public DqModel newModel(ModelInput input) {
		if ( hasModelWithName(input.getBasic().getName()) ) {
			throw new BarkDbOperationException("Record already existing");
		}
		
		try {
		    DqModel entity = createModel(input);
		    
		    String content = contentOf(input);
		    entity.setModelContent(content);

		    enhance(entity, input);
			return dqModelRepo.update(entity);
		} catch (Exception e) {
			logger.error(e.toString());
			throw new BarkDbOperationException("Failed to create a new Model", e);
		}

	}
	
    protected abstract String contentOf(ModelInput input);

    protected abstract void enhance(DqModel entity, ModelInput input);

    protected DqModel createModel(ModelInput input) {
        DqModel entity = new DqModel();
        entity.set_id(dqModelRepo.getNextId());
        entity.setModelId(input.getBasic().getName());
        entity.setModelName(input.getBasic().getName());
        entity.setNotificationEmail(input.getBasic().getEmail());
        entity.setOwner(input.getBasic().getOwner());
        entity.setSchedule(input.getBasic().getScheduleType());
        entity.setSystem(input.getBasic().getSystem());
        entity.setThreshold(input.getBasic().getThreshold());
        entity.setModelDesc(input.getBasic().getDesc());
        entity.setTimestamp(new Date().getTime());
        entity.setAssetName(input.getBasic().getDataaset());
        entity.setAssetId(input.getBasic().getDataasetId());
        entity.setReferenceModel("");
        entity.setModelType(input.getBasic().getType());

        if (input.getBasic().getStarttime() == 0) {
            entity.setStarttime(new Date().getTime());
        } else {
            entity.setStarttime(input.getBasic().getStarttime());
        }

        return entity;
    }
    protected DqModel createCountModel(ModelInput input) {
        DqModel countModel = dqModelRepo.findCountModelByAssetID(input.getBasic().getDataasetId());
        if (countModel != null) {
            return countModel;
        } 

        DataAsset asset = dataAssetRepo.getById(new Long(input.getBasic().getDataasetId()));
        ModelBasicInputNew basic = new ModelBasicInputNew();
        ModelExtraInputNew extra = new ModelExtraInputNew();
        basic.setDataaset(input.getBasic().getDataaset());
        basic.setDataasetId(input.getBasic().getDataasetId());
        basic.setDesc("Count for " + input.getBasic().getDataaset());
        basic.setEmail(input.getBasic().getEmail());
        basic.setName("Count_" + input.getBasic().getName() );
        basic.setOwner(input.getBasic().getOwner());
        basic.setScheduleType(input.getBasic() .getScheduleType());
        basic.setStatus(input.getBasic().getStatus());
        basic.setSystem(input.getBasic().getSystem());
        basic.setType(ModelType.VALIDITY);

        extra.setVaType(ValidityType.TOTAL_COUNT);
        extra.setSrcDataSet(asset.getSystem());
        extra.setSrcDb(asset.getPlatform());

        ModelInput tempCountModel = new ModelInput();
        tempCountModel.setBasic(basic);
        tempCountModel.setExtra(extra);
        return newModel(tempCountModel);
    }

    void newSampleJob4Model(DqModel input) {
		int type = input.getSchedule();
        Calendar c = Calendar.getInstance();
        Date date = new Date();
        date.setMinutes(0);
        date.setSeconds(0);
        c.setTime(date);

        for (int i = 0; i < MIN_TESTING_JOB_NUMBER; i++) {
            if (type == ScheduleType.DAILY)
                c.add(Calendar.DATE, -1);
            else if (type == ScheduleType.HOURLY)
                c.add(Calendar.HOUR, -1);
            else if (type == ScheduleType.WEEKLY)
                c.add(Calendar.DATE, -7);
            else if (type == ScheduleType.MONTHLY)
                c.add(Calendar.MONTH, -1);
            else
                continue;

            long starttime = c.getTime().getTime() / 1000 * 1000;

            DqJob job = new DqJob();
            job.setModelList(input.getModelName());
            job.setStarttime(starttime);
            job.setStatus(0);
            job.setId(input.getModelName() + "_" + starttime);
            job.setJobType(input.getModelType());

            if (jobRepo.newJob(job) == 0) {
                logger.warn("===================new job failure");
                continue;
            }
        }
    }

}
