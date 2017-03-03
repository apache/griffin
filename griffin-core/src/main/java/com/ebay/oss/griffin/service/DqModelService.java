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

import java.util.List;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.error.BarkDbOperationException;
import com.ebay.oss.griffin.vo.DqModelVo;
import com.ebay.oss.griffin.vo.ModelInput;

public interface DqModelService {

	public List<DqModelVo> getAllModles();

	// FIXME what the return value means
	// HACK why need to delete a model
	public int deleteModel(String name) throws BarkDbOperationException;

	public DqModel getGeneralModel(String name);


	public ModelInput getModelByName(String name) throws BarkDbOperationException;

	public void enableSchedule4Model(DqModel input);
	
	/**
	 * Create a new Model
	 * @param input
	 * @return 0 if successful, -1 if already existing, other negative values for other reasons
	 */
	public DqModel newModel(ModelInput input) throws BarkDbOperationException;
}
