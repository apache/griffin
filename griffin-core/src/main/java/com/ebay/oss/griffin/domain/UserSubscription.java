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
package com.ebay.oss.griffin.domain;

import java.util.List;

import com.ebay.oss.griffin.vo.PlatformSubscription;
import com.ebay.oss.griffin.vo.SystemSubscription;

/**
 * User could define the model/metrics by subscribe the DataAssets.
 */
// FIXME uid+List<asset> is enough, group by platform/system is just a rendering issue, which should
// not impact the model design.
public class UserSubscription {
    // same as ntaccount?
	String _id;

	String ntaccount;

	List<PlatformSubscription> subscribes;

	public UserSubscription() { }

	public UserSubscription(String user) {
		ntaccount = user;
	}

	public String getId() {
		return _id;
	}

	public void setId(String _id) {
		this._id = _id;
	}

	public String getNtaccount() {
		return ntaccount;
	}

	public void setNtaccount(String ntaccount) {
		this.ntaccount = ntaccount;
	}

	public List<PlatformSubscription> getSubscribes() {
		return subscribes;
	}

	public void setSubscribes(List<PlatformSubscription> subscribes) {
		this.subscribes = subscribes;
	}

	public boolean isPlatformSelected(String platform) {
		for(PlatformSubscription item : subscribes) {
			if(item.getPlatform().equals(platform) && item.isSelectAll()) {
			    return true;
			}
		}
		return false;
	}

	public boolean isSystemSelected(String platform, String system) {
		for(PlatformSubscription item : subscribes) {
			if(item.getPlatform().equals(platform)) {
				for(SystemSubscription eachSystem : item.getSystems()) {
					if(eachSystem.getSystem().equals(system) && eachSystem.isSelectAll()) {
					    return true;
					}
				}
			}
		}
		return false;
	}

	public boolean isDataAssetSelected(String platform, String system, String dataasset) {
		for(PlatformSubscription item : subscribes) {
			if(item.getPlatform().equals(platform)) {
				for(SystemSubscription eachSystem : item.getSystems()) {
					if(eachSystem.getSystem().equals(system) 
					    && eachSystem.getDataassets().contains(dataasset)) {
						    return true;
                    }
				}
			}
		}
		return false;
	}

}