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

import com.ebay.oss.griffin.domain.DqMetricsValue;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Repository
public class DqMetricsRepoImpl extends BaseIdRepo<DqMetricsValue> implements DqMetricsRepo {
    
    public DqMetricsRepoImpl() {
        super("dq_metrics_values", "DQ_METRICS_SEQ_NO", DqMetricsValue.class);
    }

	@Override
    public void update(DqMetricsValue entity, DBObject old) {
		Gson gson = new Gson();
		// DBObject t1 = gson.fromJson(gson.toJson(entity),
		// BasicDBObject.class);
		DBObject t1 = (DBObject) JSON.parse(gson.toJson(entity));
		dbCollection.update(old, t1);
	}

	@Override
    public DqMetricsValue getLatestByAssetId(String assetId) {
		DBCursor temp = dbCollection.find(new BasicDBObject("assetId",
				assetId));
		List<DBObject> all = temp.toArray();
		long latest = 0L;
		DBObject latestObject = null;
		for (DBObject o : all) {
			if (Long.parseLong(o.get("timestamp").toString()) - latest > 0) {
				latest = Long.parseLong(o.get("timestamp").toString());
				latestObject = o;
			}
		}

		if (latestObject == null) {
			return null;
		}
		return new DqMetricsValue(
		                latestObject.get("metricName").toString(),
		                Long.parseLong(latestObject.get("timestamp").toString()),
		                Float.parseFloat(latestObject.get("value").toString()));
	}

	@Override
    public List<DqMetricsValue> getByMetricsName(String name) {
		DBCursor temp = dbCollection.find(new BasicDBObject("metricName",
				name));
		List<DBObject> all = temp.toArray();
		List<DqMetricsValue> result = new ArrayList<DqMetricsValue>();
		for (DBObject o : all) {
			result.add( toEntity(o));
		}
		return result;
	}


//			                new DqMetricsValue(o.get("metricName").toString(), Long
//					.parseLong(o.get("timestamp").toString()), Float
//					.parseFloat(o.get("value").toString())));
}
