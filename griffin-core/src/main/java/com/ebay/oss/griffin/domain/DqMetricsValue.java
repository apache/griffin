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


import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import com.ebay.oss.griffin.vo.BaseObj;
import com.google.code.morphia.annotations.Id;


/**
 * Metrics is the value of a DQ perspective (DqModel) execution/calculation.
 * 
 * TODO: should introduce UnitOfMeasure in, so that a Metrics could be render independently.
 */
// uniq{modelName, timestamp}
public class DqMetricsValue extends BaseObj implements Comparable<DqMetricsValue> {

    @Id
	private Long _id;

	// model.name
	@NotNull
	@Pattern(regexp="\\A([0-9a-zA-Z\\_\\-\\.])+$")
	private String metricName;

	private String assetId;

	@Min(0)
	private long timestamp;

	@Min(0)
	private float value;

	public DqMetricsValue() { }

	public DqMetricsValue(String name, long timestamp, float value) {
		this.metricName = name;
		this.timestamp = timestamp;
		this.value = value;
	}

    public Long get_id() {
		return _id;
	}

	public void set_id(Long _id) {
		this._id = _id;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public String getAssetId() {
		return assetId;
	}

	public void setAssetId(String assetId) {
		this.assetId = assetId;
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

	@Override
	public int compareTo(DqMetricsValue o) {
	    return (int) Math.signum(o.getTimestamp() - this.getTimestamp());
//		return o.getTimestamp() == this.getTimestamp() ? 0 :
//			(o.getTimestamp() > this.getTimestamp() ? 1 : -1);
	}

    @Override
    public String toString() {
        return "DqMetricsValue [_id=" + _id + ", metricName=" + metricName + ", timestamp="
                        + timestamp + ", value=" + value + "]";
    }

}