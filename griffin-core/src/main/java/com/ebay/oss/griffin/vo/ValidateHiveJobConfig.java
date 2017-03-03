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
package com.ebay.oss.griffin.vo;

import java.util.ArrayList;
import java.util.List;

public class ValidateHiveJobConfig {
	public String dataSet;
	public List<ValidateHiveJobConfigLv1Detail> validityReq = new ArrayList<ValidateHiveJobConfigLv1Detail>();
	public List<PartitionConfig> timePartitions = new ArrayList<PartitionConfig>();

	public ValidateHiveJobConfig() { }

	public ValidateHiveJobConfig(String dataSet) {
		this.dataSet = dataSet;
	}

	public String getDataSet() {
		return dataSet;
	}

	public void setDataSet(String dataSet) {
		this.dataSet = dataSet;
	}

	public List<ValidateHiveJobConfigLv1Detail> getValidityReq() {
		return validityReq;
	}

	public void setValidityReq(List<ValidateHiveJobConfigLv1Detail> validityReq) {
		this.validityReq = validityReq;
	}

	public List<PartitionConfig> getTimePartitions() {
		return timePartitions;
	}

	public void setTimePartitions(List<PartitionConfig> timePartitions) {
		this.timePartitions = timePartitions;
	}

	public void addColumnCalculation(int colId, String colName, int type)
	{
		if(validityReq == null) validityReq = new ArrayList<ValidateHiveJobConfigLv1Detail>();
		int lv1index = 0;
		for(ValidateHiveJobConfigLv1Detail tempValidateHiveJobConfigLv1Detail : validityReq)
		{
			if(tempValidateHiveJobConfigLv1Detail.getColId() == colId && tempValidateHiveJobConfigLv1Detail.getColName().equals(colName))
			{
				lv1index = 1;
				tempValidateHiveJobConfigLv1Detail.getMetrics().add(new ValidateHiveJobConfigLv2Detail(type));
			}
		}
		if(lv1index == 0)
		{
			List<ValidateHiveJobConfigLv2Detail> newMetrics = new ArrayList<ValidateHiveJobConfigLv2Detail>();
			newMetrics.add(new ValidateHiveJobConfigLv2Detail(type));
			ValidateHiveJobConfigLv1Detail newValidateHiveJobConfigLv1Detail = new ValidateHiveJobConfigLv1Detail(colId, colName, newMetrics);
			validityReq.add(newValidateHiveJobConfigLv1Detail);
		}
	}

	public long getValue(String colName, int type)
	{
		long result = Long.MIN_VALUE;
		for(ValidateHiveJobConfigLv1Detail tempValidateHiveJobConfigLv1Detail : validityReq)
		{
			if(tempValidateHiveJobConfigLv1Detail.getColName().equals(colName))
			{
				for(ValidateHiveJobConfigLv2Detail tempValidateHiveJobConfigLv2Detail : tempValidateHiveJobConfigLv1Detail.getMetrics())
				{
					if(tempValidateHiveJobConfigLv2Detail.getName()==type)
					{
						return (long) tempValidateHiveJobConfigLv2Detail.getResult();
					}
				}
			}
		}

		return result;
	}




}
