package org.apache.griffin.core.metric;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class MetricStoreImpl implements MetricStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricStoreImpl.class);

    private RestClient client;

    public MetricStoreImpl(@Value("${elasticsearch.host}") String host, @Value("${elasticsearch.port}") int port) {
        client = RestClient.builder(new HttpHost(host, port, "http")).build();
    }

    @Override
    public List<MetricValue> getMetricValues(String metricName, int size) {
        String queryString = String.format("{\"query\": {  \"bool\":{\"filter\":[ {\"term\" : {\"name.keyword\": \"%s\" }}]}},  " +
                "\"sort\": [{\"tmst\": {\"order\": \"desc\"}}],\"size\":%d}", metricName, size);
        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
        List<MetricValue> metricValues = new ArrayList<>();
        try {
            Response response = client.performRequest("GET", "/griffin/accuracy/_search?filter_path=hits.hits._source", Collections.emptyMap(),
                    entity, new BasicHeader("Content-Type", "application/json"));
            JsonNode jsonNode = getJsonNode(response);
            if (jsonNode.hasNonNull("hits") && jsonNode.get("hits").hasNonNull("hits")) {
                for (JsonNode node : jsonNode.get("hits").get("hits")) {
                    MetricValue metricValue = getMetricValueFromJsonNode(node);
                    if (metricValue != null) {
                        metricValues.add(metricValue);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Get response from elasticsearch failed", e.getMessage());
        }
        return metricValues;
    }

    @Override
    public String addMetricValues(List<MetricValue> metricValues) {
        try {
            int failedCount = 0;
            for (MetricValue metricValue : metricValues) {
                HttpEntity entity = new NStringEntity(JsonUtil.toJson(metricValue), ContentType.APPLICATION_JSON);
                Response response = client.performRequest("POST", "/griffin/accuracy", Collections.emptyMap(), entity,
                        new BasicHeader("Content-Type", "application/json"));
                JsonNode jsonNode = getJsonNode(response);
                int failed = jsonNode.get("_shards").get("failed").asInt();
                if (failed != 0) {
                    failedCount++;
                }
            }
            if (failedCount == 0) {
                return String.format("Add metric values successful");
            } else {
                return String.format("%d records has failure occur in shards.", failedCount);
            }
        } catch (Exception e) {
            LOGGER.error("Post to elasticsearch failed", e.getMessage());
            return "Add metric values failed.";
        }
    }

    @Override
    public String deleteMetricValues(String metricName) {
        String queryString = String.format("{\"query\": {  \"bool\":{\"filter\":[ {\"term\" : {\"name.keyword\": \"%s\" }}]}}}", metricName);
        HttpEntity entity = new NStringEntity(queryString, ContentType.APPLICATION_JSON);
        try {
            Response response = client.performRequest("POST", "/griffin/accuracy/_delete_by_query", Collections.emptyMap(),
                    entity, new BasicHeader("Content-Type", "application/json"));
            JsonNode jsonNode = getJsonNode(response);
            String total = jsonNode.get("total").toString();
            String deleted = jsonNode.get("deleted").toString();
            return String.format("%s record(s) matched, %s deleted", total, deleted);
        } catch (Exception e) {
            LOGGER.error("Delete by query failed", e.getMessage());
        }
        return "Delete metric values failed";
    }

    private static JsonNode getJsonNode(Response response) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String responseStr = EntityUtils.toString(response.getEntity());
        return mapper.readTree(responseStr);
    }

    private MetricValue getMetricValueFromJsonNode(JsonNode node) throws Exception {
        JsonNode sourceNode = node.get("_source");
        if (sourceNode.isNull()) {
            return null;
        }
        Map<String, Object> source = JsonUtil.toEntity(sourceNode.toString(), new TypeReference<Map<String, Object>>() {
        });
        return new MetricValue(source.get("name").toString(), Long.parseLong(source.get("tmst").toString()), (Map<String, Object>) source.get("value"));
    }
}
