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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@SuppressWarnings({"unchecked", "rawtypes"})
@Repository
public class SequenceRepoImpl extends BaseRepo implements SequenceRepo {

    public SequenceRepoImpl() {
	    super("SEQUENCES", Object.class);
    }

	@Override
    public synchronized Long getNextSequence(String seq) {
		DBObject temp = dbCollection.findOne(new BasicDBObject("_id", seq));

		BasicDBObject document = new BasicDBObject();
		document.put("_id", seq);
		document.put(seq, Long.parseLong(temp.get(seq).toString()) + 1);

		dbCollection.save(document);

		return new Long((Long.parseLong(temp.get(seq).toString()) + 1));
	}

}
