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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Property;
import com.mongodb.DBObject;

/**
 * A DataAsset is an abstraction of a data source, from which a same type of data comes. A DataAsset
 * could be a 'table' in RDB or a topic/feed from a stream.
 */
@Entity("data_assets")
// unique constraint: asssetName + assetType + system
public class DataAsset extends IdEntity{

    // HDP the only one
	@Property("platform")
	private String platform;

	// SystemTypeConstants
	@Property("system")
	private String system;

	// e.g. viewitem
	@Property("assetName")
	private String assetName;

	// HiveTable the only one
	@Property("assetType")
	private String assetType;

	@Property("assetHDFSPath")
	private String assetHDFSPath;

	@Property("owner")
	private String owner;

	// createdDate
	@Property("timestamp")
	private Date timestamp;


	// home-made schema, DataSchema is atually a simple schemaItem, a schema consists of a list of 
	// schemaItem 
	@Embedded
	private List<DataSchema> schema = new ArrayList<>();

	// hive table partition format
	@Embedded
	private List<PartitionFormat> partitions;

	public DataAsset() { }

	// FIXME should be somewhere else, ORM
	@SuppressWarnings({"unchecked", "deprecation"})
    public DataAsset(DBObject o) {
		this.set_id(Long.parseLong(o.get("_id").toString()));
		this.setAssetHDFSPath((String)o.get("assetHDFSPath"));
		this.setAssetName((String)o.get("assetName"));
		this.setAssetType((String)o.get("assetType"));

		this.setOwner((String)o.get("owner"));
		this.setPlatform((String)o.get("platform"));
		this.setSystem((String)o.get("system"));

//		this.setPartitions((List<PartitionFormat>) o.get("partitions")); // this doesn't work
	    if(o.get("partitions")!=null) {
	        List<PartitionFormat> partitionlist = new ArrayList<PartitionFormat>();
	        List<DBObject> tlist = (List<DBObject>) o.get("partitions");
	        for(DBObject temp : tlist) {
	            partitionlist.add(new PartitionFormat(temp.get("name").toString(), temp.get("format").toString()));
	        }
	        this.setPartitions(partitionlist);
	    }

//		this.setSchema((List<DataSchema>) o.get("schema"));
		if(o.get("schema")!=null) {
		    List<DBObject> tlist = (List<DBObject>) o.get("schema");
		    List<DataSchema> list = new ArrayList<DataSchema>();
		    for(DBObject temp : tlist) {
		        list.add(new DataSchema(temp.get("name").toString(), temp.get("type").toString(), temp.get("desc").toString(), temp.get("sample").toString()));
		    }
		    this.setSchema(list);
		}

        if (!o.containsField("timestamp")) {
            this.setTimestamp(new Date());
        } else {
            this.setTimestamp(new Date(o.get("timestamp").toString()));
        }
	}
	
	{
	    
	    
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getSystem() {
		return system;
	}

	public void setSystem(String system) {
		this.system = system;
	}

	public String getAssetName() {
		return assetName;
	}

	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}

	public String getAssetType() {
		return assetType;
	}

	public void setAssetType(String assetType) {
		this.assetType = assetType;
	}

	public String getAssetHDFSPath() {
		return assetHDFSPath;
	}

	public void setAssetHDFSPath(String assetHDFSPath) {
		this.assetHDFSPath = assetHDFSPath;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public List<DataSchema> getSchema() {
		return schema;
	}

	public void setSchema(List<DataSchema> schema) {
		this.schema = schema;
	}

	public List<PartitionFormat> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<PartitionFormat> partitions) {
		this.partitions = partitions;
	}

    public int getColId(String colName) {
        if(schema == null || schema.isEmpty()) {
            return -1;
        }

        for (int i = 0; i < schema.size(); i++) {
            if (colName.equals(schema.get(i).getName()))
                return i;
        }
        return -1;
    }
}
