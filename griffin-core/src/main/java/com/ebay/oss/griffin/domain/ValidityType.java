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

/** the type of Validity model */
public class ValidityType {
	public static final int DEFAULT_COUNT = 0;
	public static final int TOTAL_COUNT = 1;
	public static final int NULL_COUNT = 2;
	public static final int UNIQUE_COUNT = 3;
	public static final int DUPLICATE_COUNT = 4;
	public static final int MAXIMUM = 5;
	public static final int MINIMUM = 6;
	public static final int MEAN = 7;
	public static final int MEDIAN = 8;
	public static final int REGULAR_EXPRESSION_MATCHING = 9;
	public static final int PATTERN_FREQUENCY = 10;

}
