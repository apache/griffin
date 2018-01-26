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

import org.apache.griffin.core.measure.entity.DataSource;
import org.apache.griffin.core.measure.entity.GriffinMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MeasureUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureUtil.class);

    public static boolean isValid(GriffinMeasure measure) {
        return getConnectorNamesIfValid(measure) != null;
    }

    public static List<String> getConnectorNamesIfValid(GriffinMeasure measure) {
        Set<String> sets = new HashSet<>();
        List<DataSource> sources = measure.getDataSources();
        for (DataSource source : sources) {
            source.getConnectors().stream().filter(dc -> dc.getName() != null).forEach(dc -> sets.add(dc.getName()));
        }
        if (sets.size() == 0 || sets.size() < sources.size()) {
            LOGGER.warn("Connector names cannot be repeated or empty.");
            return null;
        }
        return new ArrayList<>(sets);
    }
}
