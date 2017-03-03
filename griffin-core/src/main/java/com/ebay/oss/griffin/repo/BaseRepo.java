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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.oss.griffin.common.Pair;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public abstract class BaseRepo<T> implements BarkRepo<T> {

	protected static Logger logger = LoggerFactory.getLogger(BaseRepo.class);

	protected final DBCollection dbCollection;

    private final Class<T> clz;

    protected BaseRepo(String collectionName, Class<T> clz) throws RuntimeException {
        this.clz = clz;
	    Properties env = new Properties();
	    try {
            env.load(Thread.currentThread().getContextClassLoader()
                            .getResourceAsStream("application.properties"));
            String mongoServer = env.getProperty("spring.data.mongodb.host");
            int mongoPort = Integer.parseInt(env
                            .getProperty("spring.data.mongodb.port"));

            DB db = new MongoClient(mongoServer, mongoPort).getDB("unitdb0");
            
            dbCollection = db.getCollection(collectionName);
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    final public List<T> getAll() {
        List<T> result = new LinkedList<T>();
        for(DBObject dbo : dbCollection.find()) {
            result.add(toEntity(dbo));
        }
        return result;

    }

    protected T toEntity(DBObject dbo) {
        Gson gson = new Gson();
        return gson.fromJson(dbo.toString(), clz);
    }

	@Override
    final public void save(T t) {
		Gson gson = new Gson();
		DBObject t1 = (DBObject) JSON.parse(gson.toJson(t));
		dbCollection.save(t1);
	}

	@Override
    final public DBObject getByCondition(List<Pair> queryList) {
		BasicDBObject query = new BasicDBObject();
		for (Pair k : queryList) {
			query.put(k.key, k.value);
		}

		return dbCollection.findOne(query);
	}

}
