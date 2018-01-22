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
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;

import static org.junit.Assert.*;

public class PropertiesUtilTest {

    @Test
    public void testGetPropertiesForSuccess() {
        String path = "/quartz.properties";
        Properties properties = PropertiesUtil.getProperties(path, new ClassPathResource(path));
        assertEquals(properties.get("org.quartz.jobStore.isClustered"), "true");
    }

    @Test
    public void testGetPropertiesForFailureWithWrongPath() {
        String path = ".././quartz.properties";
        Properties properties = PropertiesUtil.getProperties(path, new ClassPathResource(path));
        assertEquals(properties, null);
    }

}