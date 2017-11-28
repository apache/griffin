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

package org.apache.griffin.core.job;

import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.util.FSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.griffin.core.job.JobInstance.PATH_CONNECTOR_CHARACTER;

public class FileExistPredicator implements Predicator {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileExistPredicator.class);

    public static final String PREDICT_PATH = "path";
    public static final String PREDICT_ROOT_PATH = "root.path";

    private SegmentPredicate predicate;

    public FileExistPredicator(SegmentPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean predicate() throws IOException {
        Map<String, String> config = predicate.getConfigMap();
        String[] paths = null;
        if (config.get(PREDICT_PATH) != null) {
            paths = config.get(PREDICT_PATH).split(PATH_CONNECTOR_CHARACTER);
        }
        String rootPath = config.get(PREDICT_ROOT_PATH);
        if (paths == null || rootPath == null) {
            throw new NullPointerException("Predicts path null.Please check predicts config root.path and path.");
        }
        for (String path : paths) {
            String hdfsPath = rootPath + path;
            LOGGER.info("Predict path:{}", hdfsPath);
            if (!FSUtil.isFileExist(hdfsPath)) {
                LOGGER.info(hdfsPath + " return false.");
                return false;
            }
        }
        return true;
    }
}
