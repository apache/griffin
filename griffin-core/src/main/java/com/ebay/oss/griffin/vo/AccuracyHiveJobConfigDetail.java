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


public class AccuracyHiveJobConfigDetail {
	public int sourceColId;
	public String sourceColName;
	public int targetColId;
	public String targetColName;
	public String matchFunction;
	public boolean isPK;

	public AccuracyHiveJobConfigDetail() { }

	public AccuracyHiveJobConfigDetail(int sourceColId, String sourceColName, int targetColId, String targetColName, String matchFunction, boolean isPK)
	{
		this.sourceColId = sourceColId;
		this.sourceColName = sourceColName;
		this.targetColId = targetColId;
		this.targetColName = targetColName;
		this.matchFunction = matchFunction;
		this.isPK = isPK;
	}

	public int getSourceColId() {
		return sourceColId;
	}

	public void setSourceColId(int sourceColId) {
		this.sourceColId = sourceColId;
	}

	public String getSourceColName() {
		return sourceColName;
	}

	public void setSourceColName(String sourceColName) {
		this.sourceColName = sourceColName;
	}

	public int getTargetColId() {
		return targetColId;
	}

	public void setTargetColId(int targetColId) {
		this.targetColId = targetColId;
	}

	public String getTargetColName() {
		return targetColName;
	}

	public void setTargetColName(String targetColName) {
		this.targetColName = targetColName;
	}

	public String getMatchFunction() {
		return matchFunction;
	}

	public void setMatchFunction(String matchFunction) {
		this.matchFunction = matchFunction;
	}

	public boolean isPK() {
		return isPK;
	}

	public void setPK(boolean isPK) {
		this.isPK = isPK;
	}


}
