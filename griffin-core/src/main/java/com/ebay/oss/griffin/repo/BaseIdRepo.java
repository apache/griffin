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

import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public abstract class BaseIdRepo<T> extends BaseRepo<T> implements BarkIdRepo<T> {

	@Autowired
	private SequenceRepo seqRepo;
	
	private final String idKey;
	
	protected BaseIdRepo(String collectionName, String idKey, Class<T> clz) {
	    super(collectionName, clz);
	    this.idKey = idKey;
    }
	
	@Override
    synchronized final public T getById(Long id) {
		DBObject o = dbCollection.findOne(new BasicDBObject("_id", id));
		return o != null ? toEntity(o) : null;
	}

	@Override
    final public void delete(Long id) {
		DBObject temp = dbCollection.findOne(new BasicDBObject("_id", id));
		if (temp != null) {
			dbCollection.remove(temp);
		}
	}

    @Override
    final public Long getNextId() {
        return seqRepo.getNextSequence(idKey);
    }
}
