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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtil.class);

    public static Long str2Long(String timeStr) {
        if (timeStr == null) {
            LOGGER.error("Time string can not be empty.");
            return 0L;
        }
        String trimTimeStr = timeStr.trim();
        boolean positive = true;
        if (trimTimeStr.startsWith("-")) {
            trimTimeStr = trimTimeStr.substring(1);
            positive = false;
        }

        String timePattern = "(?i)\\d+(ms|s|m|h|d)";
        Pattern pattern = Pattern.compile(timePattern);
        Matcher matcher = pattern.matcher(trimTimeStr);
        List<String> list = new ArrayList<>();
        while (matcher.find()) {
            String group = matcher.group();
            list.add(group.toLowerCase());
        }
        long time = 0;
        for (int i = 0; i < list.size(); i++) {
            long t = milliseconds(list.get(i).toLowerCase());
            if (positive) {
                time += t;
            } else {
                time -= t;
            }
        }
        return time;
    }

    private static Long milliseconds(String str) {
        try {
            if (str.endsWith("ms")) {
                return milliseconds(Long.parseLong(str.substring(0, str.length() - 2)), TimeUnit.MILLISECONDS);
            } else if (str.endsWith("s")) {
                return milliseconds(Long.parseLong(str.substring(0, str.length() - 1)), TimeUnit.SECONDS);
            } else if (str.endsWith("m")) {
                return milliseconds(Long.parseLong(str.substring(0, str.length() - 1)), TimeUnit.MINUTES);
            } else if (str.endsWith("h")) {
                return milliseconds(Long.parseLong(str.substring(0, str.length() - 1)), TimeUnit.HOURS);
            } else if (str.endsWith("d")) {
                return milliseconds(Long.parseLong(str.substring(0, str.length() - 1)), TimeUnit.DAYS);
            } else {
                LOGGER.error("Time string format error.It only supports d(day),h(hour),m(minute),s(second),ms(millsecond).Please check your time format.)");
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            LOGGER.error("Parse exception occur. {}",e);
            return 0L;
        }
    }

    private static Long milliseconds(long duration, TimeUnit unit) {
        return unit.toMillis(duration);
    }

    public static String format(String timeFormat, long time) {
        String timePattern = "#(?:\\\\#|[^#])*#";
        Date t = new Date(time);
        Pattern ptn = Pattern.compile(timePattern);
        Matcher matcher = ptn.matcher(timeFormat);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String group = matcher.group();
            String content = group.substring(1, group.length() - 1);
            String pattern = refreshEscapeHashTag(content);
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            matcher.appendReplacement(sb, sdf.format(t));
        }
        matcher.appendTail(sb);
        String endString = refreshEscapeHashTag(sb.toString());
        return endString;
    }

    private static String refreshEscapeHashTag(String str) {
        String escapeHashTagPattern = "\\\\#";
        String hashTag = "#";
        return str.replaceAll(escapeHashTagPattern, hashTag);
    }

}
