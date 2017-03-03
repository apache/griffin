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
package com.ebay.oss.griffin.service;

import java.util.Date;

import org.springframework.stereotype.Component;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.vo.DqModelVo;

@Component("modelVoConverter")
public class DqModelConverter implements Converter<DqModel, DqModelVo>{

    @Override
	public DqModelVo voOf(DqModel o) {
        if(o == null) {
            return null;
        }

	    DqModelVo vo = new DqModelVo();
	    vo.setName(o.getModelName());
	    vo.setSystem(o.getSystem());
	    vo.setDescription(o.getModelDesc());
	    vo.setType(o.getModelType());
        vo.setCreateDate(new Date(o.getTimestamp()));
        vo.setStatus("" + o.getStatus());
        vo.setAssetName(o.getAssetName());
        vo.setOwner(o.getOwner());
        return vo;
    }

    @Override
    public DqModel entityOf(DqModelVo vo) {
        // TODO Auto-generated method stub
        throw new RuntimeException("not implemented yet...");
    }


}
