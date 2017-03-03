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


public class AssetLevelMetricsDetail implements Comparable<AssetLevelMetricsDetail> {

	private long timestamp;
	private float value;
	private BollingerBandsEntity bolling;
	private MADEntity MAD;
	private float comparisionValue;

	public AssetLevelMetricsDetail() { }

	public AssetLevelMetricsDetail(long timestamp, float value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public AssetLevelMetricsDetail(long timestamp, float value,
			BollingerBandsEntity bolling) {
		this.timestamp = timestamp;
		this.value = value;
		this.bolling = bolling;
	}

	public AssetLevelMetricsDetail(long timestamp, float value,
			float comparisionValue) {
		this.timestamp = timestamp;
		this.value = value;
		this.comparisionValue = comparisionValue;
	}

	public AssetLevelMetricsDetail(long timestamp, float value, MADEntity MAD) {
		this.timestamp = timestamp;
		this.value = value;
		this.MAD = MAD;
	}

	public AssetLevelMetricsDetail(BollingerBandsEntity bolling) {
		this.bolling = bolling;
	}

	public AssetLevelMetricsDetail(MADEntity MAD) {
		this.MAD = MAD;
	}

	public AssetLevelMetricsDetail(AssetLevelMetricsDetail other) {
		this.timestamp = other.getTimestamp();
		this.value = other.getValue();
		this.comparisionValue = other.getComparisionValue();
		if (other.getBolling() != null)
			this.bolling = new BollingerBandsEntity(other.getBolling()
					.getUpper(), other.getBolling().getLower(), other
					.getBolling().getMean());
		if (other.getMAD() != null)
			this.MAD = new MADEntity(other.getMAD().getUpper(), other.getMAD()
					.getLower());
	}

	public AssetLevelMetricsDetail(float comparisionValue) {
		this.comparisionValue = comparisionValue;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public BollingerBandsEntity getBolling() {
		return bolling;
	}

	public void setBolling(BollingerBandsEntity bolling) {
		this.bolling = bolling;
	}

	public float getComparisionValue() {
		return comparisionValue;
	}

	public void setComparisionValue(float comparisionValue) {
		this.comparisionValue = comparisionValue;
	}

	public MADEntity getMAD() {
		return MAD;
	}

	public void setMAD(MADEntity mAD) {
		MAD = mAD;
	}

	@Override
	public int compareTo(AssetLevelMetricsDetail o) {
		return (int) Math.signum(o.getTimestamp() - this.getTimestamp());
//		return o.getTimestamp() == this.getTimestamp() ? 0
//				: (o.getTimestamp() > this.getTimestamp() ? 1 : -1);
	}

}
