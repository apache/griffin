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

import com.ebay.oss.griffin.domain.DataAsset;
import com.google.gson.Gson;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Repository
public class DataAssetRepoImpl extends BaseIdRepo<DataAsset> implements DataAssetRepo {

	public DataAssetRepoImpl() {
	    super("data_assets", "DQ_DATA_ASSET_SEQ_NO", DataAsset.class);
    }
	
	@Override
    public void update(DataAsset entity, DBObject old) {
		Gson gson = new Gson();
		// DBObject t1 = gson.fromJson(gson.toJson(entity),
		// BasicDBObject.class);
		DBObject t1 = (DBObject) JSON.parse(gson.toJson(entity));
		dbCollection.remove(old);
		dbCollection.save(t1);
	}

    @Override
    protected DataAsset toEntity(DBObject o) {
        return new DataAsset(o);
    }

}
