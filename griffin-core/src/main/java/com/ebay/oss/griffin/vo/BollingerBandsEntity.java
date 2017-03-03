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


public class BollingerBandsEntity {
    private long upper;
    private long lower;
    private long mean;

    public BollingerBandsEntity() {}

    public BollingerBandsEntity(long upper, long lower, long mean) {
        this.upper = upper;
        this.lower = lower;
        this.mean = mean;
    }

    public long getUpper() {
        return upper;
    }

    public void setUpper(long upper) {
        this.upper = upper;
    }

    public long getLower() {
        return lower;
    }

    public void setLower(long lower) {
        this.lower = lower;
    }

    public long getMean() {
        return mean;
    }

    public void setMean(long mean) {
        this.mean = mean;
    }


    public BollingerBandsEntity clone() {
        return new BollingerBandsEntity(getUpper(), getLower(), getMean());
    }


}
