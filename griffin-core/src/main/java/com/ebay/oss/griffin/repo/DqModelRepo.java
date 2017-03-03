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

import com.ebay.oss.griffin.domain.DataAsset;
import com.ebay.oss.griffin.domain.DqModel;

public interface DqModelRepo extends BarkIdRepo<DqModel> {

    DqModel update(DqModel model);

    ///////////
    DqModel findCountModelByAssetID(long dataasetId);

    List<DqModel> getByDataAsset(DataAsset da);

    List<DqModel> getByStatus(int testing);

    DqModel findByColumn(String string, String modelid);

    DqModel findByName(String name);

    void addReference(DqModel countModel, String name);

}
