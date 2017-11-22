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

public class FileExistPredictor implements Predictor {

    private SegmentPredict predict;

    public FileExistPredictor(SegmentPredict predict) {
        this.predict = predict;
    }

    @Override
    public boolean predict() throws IOException {
        Map<String, String> config = predict.getConfigMap();
        String[] paths = config.get("path").split(";");
        String rootPath = config.get("root.path");
        if (paths == null || rootPath == null) {
            throw new NullPointerException("Predicts path null.Please check predicts config root.path and path.");
        }
        for (String path : paths) {
//            if (!FSUtil1.isFileExist("hdfs://10.149.247.250:9000/yao/_success")) {
//                return false;
//            }
            if (!FSUtil.isFileExist(rootPath + path)) {
                return false;
            }
        }
        return true;
    }
}
