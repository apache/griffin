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

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Property;

@Entity("NotificationRecord")
public class NotificationRecord {
	@Property("id")
	private int id = -1;
	@Property("timestamp")
	private long timestamp;
	@Property("owner")
	private String owner;
	@Property("operation")
	private String operation;
	@Property("target")
	private String target;
	@Property("link")
	private String link;
	@Property("name")
	private String name;

	public NotificationRecord(){
		super();
	}

	public NotificationRecord(long timestamp, String owner, String operation,
			String target, String name) {
		super();
		this.timestamp = timestamp;
		this.owner = owner;
		this.operation = operation;
		this.target = target;
		this.name = name;
	}



	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}



}
