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

import org.junit.Test;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class MetricStoreImplTest {

    @Test
    public void testBuildBasicAuthString()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = MetricStoreImpl.class.getDeclaredMethod("buildBasicAuthString", String.class, String.class);
        m.setAccessible(true);
        String authStr = (String) m.invoke(null, "user", "password");
        assertTrue(authStr.equals("Basic dXNlcjpwYXNzd29yZA=="));
    }

}