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
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
public class TimeUtilTest {


    @Test
    public void testStr2LongWithPositive() throws Exception {
        String time = "2h3m4s";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "7384000");
    }

    @Test
    public void testStr2LongWithNegative() throws Exception {
        String time = "-2h3m4s";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "-7384000");
    }

    @Test
    public void testStr2LongWithNull() throws Exception {
        String time = null;
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "0");
    }

    @Test
    public void testStr2LongWithDay() throws Exception {
        String time = "1d";
        System.out.println(TimeUtil.str2Long(time));
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "86400000");
    }
    @Test
    public void testStr2LongWithHour() throws Exception {
        String time = "1h";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "3600000");
    }

    @Test
    public void testStr2LongWithMinute() throws Exception {
        String time = "1m";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "60000");
    }

    @Test
    public void testStr2LongWithSecond() throws Exception {
        String time = "1s";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "1000");
    }

    @Test
    public void testStr2LongWithMillisecond() throws Exception {
        String time = "1ms";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "1");
    }

    @Test
    public void testStr2LongWithIllegalFormat() throws Exception {
        String time = "1y2m3s";
        assertEquals(String.valueOf(TimeUtil.str2Long(time)), "123000");
    }

    @Test
    public void testFormat() throws Exception {
        String format = "dt=#YYYYMMdd#";
        Long time = 1516186620155L;
        String timeZone = "GMT+8:00";
        assertEquals(TimeUtil.format(format,time,timeZone),"dt=20180117");
    }

    @Test
    public void testFormatWithDiff() throws Exception {
        String format = "dt=#YYYYMMdd#/hour=#HH#";
        Long time = 1516186620155L;
        String timeZone = "GMT+8:00";
        assertEquals(TimeUtil.format(format,time,timeZone),"dt=20180117/hour=18");
    }

    @Test
    public void testFormatWithIllegalException() throws Exception {
        String format = "\\#YYYYMMdd\\#";
        Long time = 1516186620155L;
        String timeZone = "GMT+8:00";
        IllegalArgumentException exception = formatException(format, time,timeZone);
        assert exception != null;
    }

    private IllegalArgumentException formatException(String format,Long time,String timeZone) {
        IllegalArgumentException exception = null;
        try {
            TimeUtil.format(format,time,timeZone);
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        return exception;
    }

}