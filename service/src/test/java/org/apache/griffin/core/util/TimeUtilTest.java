/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.util;

import org.junit.Test;

public class TimeUtilTest {
    @Test
    public void testTimeString2Long() throws Exception {
//        Long[] time = new Long[0];
        System.out.println(genSampleTimestamps("-1h", "-2h", "1").length);
    }

    private Long[] genSampleTimestamps(String offsetStr, String rangeStr, String unitStr) throws Exception {
        Long offset = TimeUtil.timeString2Long(offsetStr);
        Long range = TimeUtil.timeString2Long(rangeStr);
        Long dataUnit = TimeUtil.timeString2Long(unitStr);
        //offset usually is negative
        Long dataStartTime = 123 + offset;
        if (range < 0) {
            dataStartTime += range;
            range = Math.abs(range);
        }
        if (Math.abs(dataUnit) >= range|| dataUnit == 0) {
            return new Long[]{dataStartTime};
        }
        int count = (int) (range / dataUnit);
        Long[] timestamps = new Long[count];
        for (int index = 0; index < count; index++) {
            timestamps[index] = dataStartTime + index * dataUnit;
        }
        return timestamps;
    }
}