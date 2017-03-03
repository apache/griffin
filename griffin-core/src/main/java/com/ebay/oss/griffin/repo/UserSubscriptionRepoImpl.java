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

import com.ebay.oss.griffin.domain.UserSubscription;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

@Repository
public class UserSubscriptionRepoImpl extends BaseRepo<UserSubscription>
                implements UserSubscriptionRepo {

    private UserSubscriptionRepoImpl() {
        super("user_subscribe", UserSubscription.class);
    }

    @Override
    public void upsertUserSubscribe(UserSubscription item) {
        if (item.getNtaccount() == null)
            return;

        item.setId(item.getNtaccount());// user_subscribe

        DBObject find = dbCollection.findOne(new BasicDBObject("_id", item.getId()));
        if (find != null)
            dbCollection.remove(find);

        Gson gson = new Gson();
        DBObject t1 = (DBObject) JSON.parse(gson.toJson(item));
        dbCollection.save(t1);
    }

    @Override
    public UserSubscription getUserSubscribeItem(String user) {
        Gson gson = new Gson();
        DBObject find = dbCollection.findOne(new BasicDBObject("_id", user));

        if (find == null)
            return null;
        else
            return gson.fromJson(find.toString(), UserSubscription.class);
    }

}
