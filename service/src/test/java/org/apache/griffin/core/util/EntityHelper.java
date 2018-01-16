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


import org.apache.griffin.core.job.entity.VirtualJob;
import org.apache.griffin.core.measure.entity.*;

import java.io.IOException;
import java.util.*;

public class EntityHelper {
    public static GriffinMeasure createGriffinMeasure(String name) throws Exception {
        DataConnector dcSource = createDataConnector("source_name", "default", "test_data_src", "dt=#YYYYMMdd# AND hour=#HH#");
        DataConnector dcTarget = createDataConnector("target_name", "default", "test_data_tgt", "dt=#YYYYMMdd# AND hour=#HH#");
        return createGriffinMeasure(name, dcSource, dcTarget);
    }

    public static GriffinMeasure createGriffinMeasure(String name, DataConnector dcSource, DataConnector dcTarget) throws Exception {
        DataSource dataSource = new DataSource("source", Arrays.asList(dcSource));
        DataSource targetSource = new DataSource("target", Arrays.asList(dcTarget));
        List<DataSource> dataSources = new ArrayList<>();
        dataSources.add(dataSource);
        dataSources.add(targetSource);
        String rules = "source.id=target.id AND source.name=target.name AND source.age=target.age";
        Map<String, Object> map = new HashMap<>();
        map.put("detail", "detail info");
        Rule rule = new Rule("griffin-dsl", "accuracy", rules, map);
        EvaluateRule evaluateRule = new EvaluateRule(Arrays.asList(rule));
        return new GriffinMeasure(name, "test", dataSources, evaluateRule);
    }

    public static DataConnector createDataConnector(String name, String database, String table, String where) throws IOException {
        HashMap<String, String> config = new HashMap<>();
        config.put("database", database);
        config.put("table.name", table);
        config.put("where", where);
        return new DataConnector(name, "1h", config, null);
    }

    public static ExternalMeasure createExternalMeasure(String name) {
        return new ExternalMeasure(name, "description", "org", "test", "metricName", new VirtualJob());
    }

}
