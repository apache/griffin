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

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Property;

/**
 * this is a simple representation of a schema system, which describe the data type of a table.
 */
@Embedded
public class DataSchema{

	@Property("name")
	private String name;

	@Property("type")
	private String type;

	@Property("desc")
	private String desc;

	@Property("sample")
	private String sample;

	public DataSchema() {
	}

	public DataSchema(String name, String type, String desc, String sample) {
		this.name = name;
		this.type = type;
		this.desc = desc;
		this.sample = sample;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getSample() {
		return sample;
	}

	public void setSample(String sample) {
		this.sample = sample;
	}

}
