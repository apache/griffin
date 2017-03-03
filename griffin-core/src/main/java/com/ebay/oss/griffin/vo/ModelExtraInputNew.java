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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelExtraInputNew extends BaseObj{

	private String srcDb;

	private String srcDataSet;

	private String targetDb;

	private String targetDataSet;

	/** validityType 
	 * @see ValidityType
	 */
	private int vaType = -1;

	/** anomaly type 
	 * @see AnomalyType
	 */
	private int anType = -1;

	private String column;

	private String publishUrl;

	public String getSrcDb() { return srcDb; }
	public void setSrcDb(String srcDb) { this.srcDb = srcDb; }

	public String getSrcDataSet() { return srcDataSet; }
	public void setSrcDataSet(String srcDataSet) { this.srcDataSet = srcDataSet; }

	public String getTargetDb() { return targetDb; }
	public void setTargetDb(String targetDb) { this.targetDb = targetDb; }

	public String getTargetDataSet() { return targetDataSet; }
	public void setTargetDataSet(String targetDataSet) { this.targetDataSet = targetDataSet; }

	public int getVaType() { return vaType; }
	public void setVaType(int vaType) { this.vaType = vaType; }

	public String getColumn() { return column; }
	public void setColumn(String column) { this.column = column; }

	public String getPublishUrl() { return publishUrl; }
	public void setPublishUrl(String publishUrl) { this.publishUrl = publishUrl; }

	public int getAnType() { return anType; }
	public void setAnType(int anType) { this.anType = anType; }

}
