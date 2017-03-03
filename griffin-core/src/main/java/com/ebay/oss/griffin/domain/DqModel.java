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
import com.google.code.morphia.annotations.Property;

/**
 * It is a specification of Data Quality perspective, which defines: 
 * <ol>
 * <li> what data against, DataAsset(s) </li>
 * <li> which type of DQ, ModelType </li>
 * <li> related meta data based on the concrete modelType, modelContent </li>
 * </ul>
 */
@Entity("dq_model")
public class DqModel extends IdEntity{

    // same as modelName
	@Property("modelId")
	private String modelId;

	@Property("modelName")
	private String modelName;

	@Property("modelType")
	private int modelType;

	@Property("modelDesc")
	private String modelDesc;

	@Property("assetId")
	private long assetId;

	@Property("assetName")
	private String assetName;

	@Property("threshold")
	private float threshold;

	@Property("notificationEmail")
	private String notificationEmail;

	@Property("owner")
	private String owner;

	@Property("status")
	private int status;

	// @see ScheduleType
	@Property("schedule")
	private int schedule;

	/**
	 * <ul>
	 * <li>ModelType.ACCURACY</li>
	 * <pre>
	 * srcDb | srcDataSet | dstDB | dstDataSet | [MappingItemInput.src, MII.dst, MII.isPk, MII.matchedMethod];
	 * </pre>
	 * <li>ModelType.VALIDITY</li>
	 * <pre> srcDb | srcDataSet | validityType | column </pre>
	 * <li>ModelType.Anomaly</li>  
	 * <pre> srcDb | srcDataSet | anomalType </pre>
	 * <li>ModelType.Publish</li>
	 * <pre> publishUrl </pre>
	 */
	// FIXME should model this explicitly, ASAP. 
	@Property("modelContent")
	private String modelContent;

	// @See SystemType
	@Property("system")
	private int system;

	/**
	 * for a AnormalyDetection model need a reference model, say a count_model. So a count mound will
	 * hold a reference to Anomaly model.
	 */
	@Property("referenceModel")
	private String referenceModel;

	@Property("timestamp")
	private long timestamp;

	@Property("starttime")
	private long starttime;

	public String getModelId() {
		return modelId;
	}

	public void setModelId(String modelId) {
		this.modelId = modelId;
	}

	public String getModelName() {
		return modelName;
	}

	public void setModelName(String modelName) {
		this.modelName = modelName;
	}

	public int getModelType() {
		return modelType;
	}

	public void setModelType(int modelType) {
		this.modelType = modelType;
	}

	public String getModelDesc() {
		return modelDesc;
	}

	public void setModelDesc(String modelDesc) {
		this.modelDesc = modelDesc;
	}

	public long getAssetId() {
		return assetId;
	}

	public void setAssetId(long assetId) {
		this.assetId = assetId;
	}

	public float getThreshold() {
		return threshold;
	}

	public void setThreshold(float threshold) {
		this.threshold = threshold;
	}

	public String getNotificationEmail() {
		return notificationEmail;
	}

	public void setNotificationEmail(String notificationEmail) {
		this.notificationEmail = notificationEmail;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getModelContent() {
		return modelContent;
	}

	public void setModelContent(String modelContent) {
		this.modelContent = modelContent;
	}

	public long  getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long  timestamp) {
		this.timestamp = timestamp;
	}

	public int getSchedule() {
		return schedule;
	}

	public void setSchedule(int schedule) {
		this.schedule = schedule;
	}

	public int getSystem() {
		return system;
	}

	public void setSystem(int system) {
		this.system = system;
	}

	public String getAssetName() {
		return assetName;
	}

	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}

	public String getReferenceModel() {
		return referenceModel;
	}

	public void setReferenceModel(String referenceModel) {
		this.referenceModel = referenceModel;
	}

	public long getStarttime() {
		return starttime;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}




}
