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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.metric.model.MetricValue;
import org.apache.griffin.core.util.JsonUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class MetricStoreImpl implements MetricStore {

    private static final String INDEX = "griffin";
    private static final String TYPE = "accuracy";
    private static final String URL_BASE = "/griffin/accuracy";

    private RestClient client;
    private HttpHeaders headers;
    private String url_get;
    private String url_delete;
    private String url_post;
    private ObjectMapper mapper;

    public MetricStoreImpl(@Value("${elasticsearch.host}") String host, @Value("${elasticsearch.port}") int port) {
        client = RestClient.builder(new HttpHost(host, port, "http")).build();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        this.headers = headers;
        this.url_get = URL_BASE + "/_search?filter_path=hits.hits._source";
        this.url_post = URL_BASE + "/_bulk";
        this.url_delete = URL_BASE + "/_delete_by_query";
        this.mapper = new ObjectMapper();
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName, int from, int size) throws IOException {
        HttpEntity entity = getHttpEntityForSearch(metricName, from, size);
        Response response = client.performRequest("GET", url_get, Collections.emptyMap(), entity);
        return getMetricValuesFromResponse(response);
    }

    private HttpEntity getHttpEntityForSearch(String metricName, int from, int size) throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> queryParam = Collections.singletonMap("term", Collections.singletonMap("name.keyword", metricName));
        Map<String, Object> sortParam = Collections.singletonMap("tmst", Collections.singletonMap("order", "desc"));
        map.put("query", queryParam);
        map.put("sort", sortParam);
        map.put("from", from);
        map.put("size", size);
        return new NStringEntity(JsonUtil.toJson(map), ContentType.APPLICATION_JSON);
    }

    private List<MetricValue> getMetricValuesFromResponse(Response response) throws IOException {
        List<MetricValue> metricValues = new ArrayList<>();
        JsonNode jsonNode = mapper.readTree(EntityUtils.toString(response.getEntity()));
        if (jsonNode.hasNonNull("hits") && jsonNode.get("hits").hasNonNull("hits")) {
            for (JsonNode node : jsonNode.get("hits").get("hits")) {
                JsonNode sourceNode = node.get("_source");
                metricValues.add(new MetricValue(sourceNode.get("name").asText(), Long.parseLong(sourceNode.get("tmst").asText()),
                        JsonUtil.toEntity(sourceNode.get("value").toString(), new TypeReference<Map<String, Object>>() {
                        })));
            }
        }
        return metricValues;
    }

    @Override
    public ResponseEntity addMetricValues(List<MetricValue> metricValues) throws IOException {
        String bulkRequestBody = getBulkRequestBody(metricValues);
        HttpEntity entity = new NStringEntity(bulkRequestBody, ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", url_post, Collections.emptyMap(), entity);
        return getResponseEntityFromResponse(response);

    }

    private String getBulkRequestBody(List<MetricValue> metricValues) throws JsonProcessingException {
        String actionMetaData = String.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }\n", INDEX, TYPE);
        StringBuilder bulkRequestBody = new StringBuilder();
        for (MetricValue metricValue : metricValues) {
            bulkRequestBody.append(actionMetaData);
            bulkRequestBody.append(JsonUtil.toJson(metricValue));
            bulkRequestBody.append("\n");
        }
        return bulkRequestBody.toString();
    }


    @Override
    public ResponseEntity deleteMetricValues(String metricName) throws IOException {
        Map<String, Object> param = Collections.singletonMap("query",
                Collections.singletonMap("term", Collections.singletonMap("name.keyword", metricName)));
        HttpEntity entity = new NStringEntity(JsonUtil.toJson(param), ContentType.APPLICATION_JSON);
        Response response = client.performRequest("POST", url_delete, Collections.emptyMap(), entity);
        return getResponseEntityFromResponse(response);
    }

    private ResponseEntity getResponseEntityFromResponse(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity());
        HttpStatus status = HttpStatus.valueOf(response.getStatusLine().getStatusCode());
        return new ResponseEntity<>(body, headers, status);
    }
}
