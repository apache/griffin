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

import com.google.code.morphia.annotations.Property;

/**
 * This is a representation of model execution(s) schedule. <p/>
 * 
 * A DqSchedule has muliple DqJob, based on the ScheduleType.
 * 
 * A DqModel defines what DQ should be calculated, there 
 * will be a corresponding algo model (defined in bark-models) describing how to calculate.
 * 
 * @see DqJob
 * @see ScheduleType
 */
public class DqSchedule extends IdEntity {

    /** a list of modelName, seprated by ScheduleModelSeperator.SEPERATOR.
     * if ModelType.VALIDITY, there will be multiple model included, or only one model.
     */
	@Property("modelList")
	private String modelList;

	@Property("assetId")
	private long assetId;

	/** Inherits from DqModel.modelType. In case of modelList has more models, they should share a
	 * same modelType, specifically the VALIDITY.
	 * 
	 * @see ModelType 
	 * */
	@Property("jobType")
	private int jobType;

	// @see ScheduleType
	@Property("scheduleType")
	private int scheduleType;

	/** @see JobStatus */
	@Property("status")
	private int status;

	@Property("starttime")
	private long starttime;

	@Property("content")
	private String content;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public long getStarttime() {
		return starttime;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public int getScheduleType() {
		return scheduleType;
	}

	public void setScheduleType(int scheduleType) {
		this.scheduleType = scheduleType;
	}

	public String getModelList() {
		return modelList;
	}

	public void setModelList(String modelList) {
		this.modelList = modelList;
	}

	public long getAssetId() {
		return assetId;
	}

	public void setAssetId(long assetId) {
		this.assetId = assetId;
	}

	public int getJobType() {
		return jobType;
	}

	public void setJobType(int jobType) {
		this.jobType = jobType;
	}





}
