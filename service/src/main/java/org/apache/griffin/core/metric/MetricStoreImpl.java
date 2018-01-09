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

package org.apache.griffin.core.metric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.metric.model.MetricValue;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class MetricStoreImpl implements MetricStore {

    private RestClient client;

    private ObjectMapper mapper = new ObjectMapper();

    public MetricStoreImpl(@Value("${elasticsearch.host}") String host, @Value("${elasticsearch.port}") int port) {
        client = RestClient.builder(new HttpHost(host, port, "http")).build();
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName, int from, int size) throws Exception {
        Map<String, Object> map = new HashMap<>();
        Map queryParam = Collections.singletonMap("term", Collections.singletonMap("name.keyword", metricName));
        Map sortParam = Collections.singletonMap("tmst", Collections.singletonMap("order", "desc"));
        map.put("query", queryParam);
        map.put("sort", sortParam);
        map.put("from", from);
        map.put("size", size);
        List<MetricValue> metricValues = new ArrayList<>();
        HttpEntity entity = new NStringEntity(JsonUtil.toJson(map), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("GET", "/griffin/accuracy/_search?filter_path=hits.hits._source",
                Collections.emptyMap(), entity, new BasicHeader("Content-Type", "application/json"));
        JsonNode jsonNode = mapper.readTree(EntityUtils.toString(response.getEntity()));
        if (jsonNode.hasNonNull("hits") && jsonNode.get("hits").hasNonNull("hits")) {
            for (JsonNode node : jsonNode.get("hits").get("hits")) {
                JsonNode sourceNode = node.get("_source");
                metricValues.add(new MetricValue(sourceNode.get("name").asText(), Long.parseLong(sourceNode.get("tmst").asText()),
                        JsonUtil.toEntity(sourceNode.get("value").toString(), Map.class)));
            }
        }
        return metricValues;
    }

    @Override
    public void addMetricValue(MetricValue metricValue) throws Exception {
        HttpEntity entity = new NStringEntity(JsonUtil.toJson(metricValue), ContentType.APPLICATION_JSON);
        client.performRequest("POST", "/griffin/accuracy", Collections.emptyMap(), entity,
                new BasicHeader("Content-Type", "application/json"));

    }

    @Override
    public void deleteMetricValues(String metricName) throws Exception {
        Map param = Collections.singletonMap("query",
                Collections.singletonMap("term", Collections.singletonMap("name.keyword", metricName)));
        HttpEntity entity = new NStringEntity(JsonUtil.toJson(param), ContentType.APPLICATION_JSON);
        client.performRequest("POST", "/griffin/accuracy/_delete_by_query", Collections.emptyMap(),
                entity, new BasicHeader("Content-Type", "application/json"));
    }
}
