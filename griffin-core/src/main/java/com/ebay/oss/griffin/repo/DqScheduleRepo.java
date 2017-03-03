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

import com.ebay.oss.griffin.domain.DqSchedule;
import com.mongodb.DBObject;

public interface DqScheduleRepo extends BarkIdRepo<DqSchedule> {

    ///////////
    void updateByModelType(DqSchedule schedule, int modelType);

    // is it required???
    void updateModelType(DBObject currentSchedule, int modelType);

    void deleteByModelList(String name);

    DBObject getValiditySchedule(long assetId);

}
