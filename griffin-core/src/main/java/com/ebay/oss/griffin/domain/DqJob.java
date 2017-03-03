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

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

/**
 * A DqJob is an instance of DqSchedule, the frequency/multiplicity is based on ScheduleType.
 */
@Entity("dq_job")
public class DqJob{

    /** {modelName} + "_" + "yyyy-mm-dd xx:00:00 000".getTime() */
	@Id
	private String _id;

	// Inherits from DqSchedule.modelList
	@Property("modelList")
	private String modelList;

	/** Inherits from DqSchedule#jobType.*/
	@Property("jobType")
	private int jobType;

	/** @see JobStatus */
	@Property("status")
	private int status;

	@Property("starttime")
	private long starttime;

	@Property("content")
	private String content;

	@Property("endtime")
	private long endtime;

	@Property("value")
	private long value;

	public String getId() {
		return _id;
	}

	public void setId(String _id) {
		this._id = _id;
	}

	public String getModelList() {
		return modelList;
	}

	public void setModelList(String modelList) {
		this.modelList = modelList;
	}

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

	public long getEndtime() {
		return endtime;
	}

	public void setEndtime(long endtime) {
		this.endtime = endtime;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public int getJobType() {
		return jobType;
	}

	public void setJobType(int jobType) {
		this.jobType = jobType;
	}



}
