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

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import com.ebay.oss.griffin.common.NumberUtils;
import com.ebay.oss.griffin.domain.DqJob;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

// FIXME why DqJob use a string id
@Repository
public class DqJobRepoImpl extends BaseRepo<DqJob> implements DqJobRepo {
    
    public DqJobRepoImpl() {
        super("dq_job", DqJob.class);
    }
    
	@Override
    public void update(DqJob job) {
		DBObject dbo = dboOf(job);
		if (dbo != null)
			dbCollection.remove(dbo);

		save(job);
	}

	DBObject dboOf(DqJob job) {
	    return dbCollection.findOne(new BasicDBObject("_id", job.getId()));
	}
	@Override
    public int newJob(DqJob job) {
		try {
			DBObject temp = dboOf(job);
			if (temp != null)
				return 0;
			Gson gson = new Gson();
			DBObject t1 = (DBObject) JSON.parse(gson.toJson(job));
			dbCollection.save(t1);
			return 1;
		} catch (Exception e) {
			logger.warn("===========insert new job error==============="
					+ e.getMessage());
			return 0;
		}
	}

    @Override
    public DqJob getById(String jobId) {
        DBObject dbo = dbCollection.findOne(new BasicDBObject("_id", jobId));
        if (dbo == null) {
            return null;
        }
        return toEntity(dbo);
    }

    @Override
    protected DqJob toEntity(DBObject o) {
        DqJob entity = new DqJob();
        entity.setId((String)o.get("_id"));
        entity.setModelList((String)o.get("modelList"));
        entity.setJobType(NumberUtils.parseInt( o.get("jobType")));
        entity.setStatus(NumberUtils.parseInt( o.get("status")));
        entity.setStarttime(NumberUtils.parseLong(o.get("starttime")));
        entity.setContent((String) o.get("content"));
        entity.setEndtime(NumberUtils.parseLong( o.get("endtime")));
        entity.setValue(NumberUtils.parseLong(o.get("value")));

        return entity;
    }

	@Override
    public List<DqJob> getByStatus(int status) {
	    List<DqJob> list = new ArrayList<>();

	    List<DBObject> dboList = dbCollection.find(new BasicDBObject("status", status)).toArray();
        for (DBObject dbo : dboList) {
            list.add(toEntity(dbo));
        }
        
        return list;
	}
}
