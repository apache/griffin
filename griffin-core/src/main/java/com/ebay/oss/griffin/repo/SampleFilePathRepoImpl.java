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

import java.util.List;

import org.springframework.stereotype.Repository;

import com.ebay.oss.griffin.domain.SampleFilePathLKP;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

@Repository
public class SampleFilePathRepoImpl extends BaseIdRepo<SampleFilePathLKP> implements SampleFilePathRepo {
    
    public SampleFilePathRepoImpl() {
	    super("dq_missed_file_path_lkp", "DQ_MISSED_FILE_SEQ_NO", SampleFilePathLKP.class);
    }
    
	@Override
    public List<DBObject> findByModelName(String name) {

		DBCursor temp = dbCollection
                            .find(new BasicDBObject("modelName", name)).limit(20)
                            .sort(new BasicDBObject("timestamp", -1));
		return temp.toArray();
	}

}
