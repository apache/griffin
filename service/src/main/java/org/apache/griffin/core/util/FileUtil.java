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

import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.griffin.core.util.JsonUtil.toJsonWithFormat;

public class FileUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);
    public static String env_batch;
    public static String env_streaming;

    public static String readEnv(String path) throws IOException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File file = new File(classLoader.getResource(path).getFile());
        return toJsonWithFormat(JsonUtil.toEntity(file, new TypeReference<Object>() {
        }));
    }

    public static String readBatchEnv(String path,String name) throws IOException {
        if (env_batch != null) {
            return env_batch;
        }
        env_batch = readEnv(path);
        return env_batch;
    }

    public static String readStreamingEnv(String path,String name) throws IOException {
        if (env_streaming != null) {
            return env_streaming;
        }
        env_streaming = readEnv(path);
        return env_streaming;
    }
}
