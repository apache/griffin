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

package com.ebay.oss.griffin.repo;

import org.springframework.stereotype.Repository;

import com.ebay.oss.griffin.domain.DqSchedule;
import com.ebay.oss.griffin.domain.ModelType;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Repository
public class DqScheudleRepoImpl extends BaseIdRepo<DqSchedule> implements DqScheduleRepo {

    public DqScheudleRepoImpl() {
	    super("dq_schedule", "DQ_JOB_SEQ_NO", DqSchedule.class);
    }
    
	@Override
    public DBObject getValiditySchedule(long assetId) {
		BasicDBObject document = new BasicDBObject();
		document.put("assetId", assetId);
		document.put("jobType", ModelType.VALIDITY);
		return dbCollection.findOne(document);
	}

	@Override
    public void updateByModelType(DqSchedule schedule, int type) {
		DBObject dbo = null;
		if (type == ModelType.ACCURACY) {
			dbo = dbCollection.findOne(new BasicDBObject("modelList", schedule.getModelList()));
		} else if (type == ModelType.VALIDITY) {
			dbo = getValiditySchedule(schedule.getAssetId());
		}

		if (dbo != null) {
			dbCollection.remove(dbo);
		}

		Gson gson = new Gson();
		DBObject t1 = (DBObject) JSON.parse(gson.toJson(schedule));
		dbCollection.save(t1);
	}

	@Override
    public void updateModelType(DBObject schedule, int type) {
		DBObject dbo = null;
		if (type == ModelType.ACCURACY) {
			dbo = dbCollection.findOne(new BasicDBObject("modelList", schedule
					.get("modelList")));
		} else if (type == ModelType.VALIDITY) {
			dbo = getValiditySchedule(Integer.parseInt(schedule.get("assetId").toString()));
		}
		if (dbo != null)
			dbCollection.remove(dbo);
		dbCollection.save(schedule);
	}

	@Override
    public void deleteByModelList(String modelList) {
		DBObject dbo = dbCollection.findOne(new BasicDBObject("modelList", modelList));
		if (dbo != null) {
			dbCollection.remove(dbo);
		}
	}

}
