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

import org.apache.griffin.core.metric.model.MetricValue;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.BDDMockito.*;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RestClient.class, RestClientBuilder.class})
@PowerMockIgnore("javax.management.*")
public class MetricStoreImplTest {

    private static final String INDEX = "griffin";
    private static final String TYPE = "accuracy";

    @Test
    public void testBuildBasicAuthString()
            throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {
        Method m = MetricStoreImpl.class.getDeclaredMethod
                ("buildBasicAuthString", String.class,
                        String.class);
        m.setAccessible(true);
        String authStr = (String) m.invoke(null, "user", "password");
        assertTrue(authStr.equals("Basic dXNlcjpwYXNzd29yZA=="));
    }

    @Test
    public void testMetricGetting() throws IOException {
        PowerMockito.mockStatic(RestClient.class);
        RestClient restClientMock = PowerMockito.mock(RestClient.class);
        RestClientBuilder restClientBuilderMock = PowerMockito.mock(RestClientBuilder.class);
        Response responseMock = PowerMockito.mock(Response.class);
        HttpEntity httpEntityMock = PowerMockito.mock(HttpEntity.class);
        InputStream is = new BufferedInputStream(new FileInputStream("/Users/dershov/IdeaProjects/griffin/service/src/test/resources/metricvalue.json"));
        Map<String, String> map =new HashMap<>();
        map.put("applicationId", "application_1549876136110_0018");

        String urlBase = String.format("/%s/%s", INDEX, TYPE);
        String urlGet = urlBase.concat("/_search?filter_path=hits.hits._source");

        given(RestClient.builder(anyVararg())).willReturn(restClientBuilderMock);
        given(restClientBuilderMock.build()).willReturn(restClientMock);
        given(restClientMock.performRequest(eq("GET"), eq(urlGet), eq(map), anyVararg())).willReturn(responseMock);
        given(responseMock.getEntity()).willReturn(httpEntityMock);
        given(httpEntityMock.getContent()).willReturn(is);

        MetricStoreImpl metricStore = new MetricStoreImpl("", 0, "", "", "");
        PowerMockito.verifyStatic();
        MetricValue metric = metricStore.getMetric("application_1549876136110_0018");

        System.out.println(metric);


    }

}
