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


public class DQHealthStats {
	private int health;
	private int warn;
	private int invalid;

	public int getHealth() {
		return health;
	}
	public void setHealth(int health) {
		this.health = health;
	}
	public int getWarn() {
		return warn;
	}
	public void setWarn(int warn) {
		this.warn = warn;
	}
	public int getInvalid() {
		return invalid;
	}
	public void setInvalid(int invalid) {
		this.invalid = invalid;
	}


}
