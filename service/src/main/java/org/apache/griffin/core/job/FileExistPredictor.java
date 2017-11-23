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

import org.apache.griffin.core.job.entity.SegmentPredict;
import org.apache.griffin.core.util.FSUtil;

import java.io.IOException;
import java.util.Map;

import static org.apache.griffin.core.job.PredictJob.PATH_CONNECTOR_CHARACTER;

public class FileExistPredictor implements Predictor {

    public static final String PREDICT_PATH = "path";
    public static final String PREDICT_ROOT_PATH = "root.path";

    private SegmentPredict predict;

    public FileExistPredictor(SegmentPredict predict) {
        this.predict = predict;
    }

    @Override
    public boolean predict() throws IOException {
        Map<String, String> config = predict.getConfigMap();
        String[] paths = null;
        if (config.get(PREDICT_PATH) != null) {
            paths = config.get(PREDICT_PATH).split(PATH_CONNECTOR_CHARACTER);
        }
        String rootPath = config.get(PREDICT_ROOT_PATH);
        if (paths == null || rootPath == null) {
            throw new NullPointerException("Predicts path null.Please check predicts config root.path and path.");
        }
        for (String path : paths) {
            if (!FSUtil.isFileExist(rootPath + path)) {
                return false;
            }
        }
        return true;
    }
}
