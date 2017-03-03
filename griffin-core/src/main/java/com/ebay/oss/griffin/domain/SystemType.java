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


/** 
 * A system is actually an application (or sth else, like data container), managing data which 
 * involved the data quality. A System could be considered a set of DataAssets. 
 * <p/>
 * here lists the different type of bark involved System. 
 */
public class SystemType {

	public static final int BULLSEYE = 0;
	public static final int GPS = 1;
	public static final int HADOOP = 2;
	public static final int PDS = 3;
	public static final int IDLS = 4;
	public static final int PULSAR = 5;
	public static final int KAFKA = 6;
	public static final int SOJOURNER= 7;
	public static final int SITESPEED = 8;
	public static final int EDW = 9;

//	private final int value;
//	
//	private final String desc;
//	
//	public SystemType(int value, String desc) {
//        super();
//        this.value = value;
//        this.desc = desc;
//    }

    private static final String[] array = {"Bullseye", "GPS", "Hadoop", "PDS", "IDLS", "Pulsar", "Kafka", "Sojourner", "SiteSpeed", "EDW"};

	public static String val(int type){
		if(type < array.length && type >=0){
			return array[type];
		}else{
			return type + "";
		}


	}

	public static int indexOf(String desc){
		for(int i = 0;i < array.length; i ++){
			if(array[i].equals(desc)){
				return i;
			}
		}

		return -1;
	}

}
