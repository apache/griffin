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

package org.apache.griffin.core.job.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * SparkJobDO
 * {
 * "file": "hdfs:///griffin/griffin-measure.jar",
 * "className": "org.apache.griffin.measure.batch.Application",
 * "args": [
 * "/benchmark/test/env.json",
 * "{\"name\":\"data_rdm\",\"type\":\"accuracy\",\"source\":{\"type\":\"hive\",\"version\":\"1.2\",\"config\":{\"database\":\"default\",\"table.name\":\"data_rdm\"} },\"target\":{\"type\":\"hive\",\"version\":\"1.2\",\"config\":{\"database\":\"default\",\"table.name\":\"data_rdm\"} },\"evaluateRule\":{\"sampleRatio\":1,\"rules\":\"$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1\"} }",
 * "hdfs,raw"
 * ],
 * "name": "griffin-livy",
 * "queue": "default",
 * "numExecutors": 2,
 * "executorCores": 4,
 * "driverMemory": "2g",
 * "executorMemory": "2g",
 * "conf": {
 * "spark.jars.packages": "com.databricks:spark-avro_2.10:2.0.1"
 * },
 * "jars": [
 * "/livy/datanucleus-api-jdo-3.2.6.jar",
 * "/livy/datanucleus-core-3.2.10.jar",
 * "/livy/datanucleus-rdbms-3.2.9.jar"
 * ],
 * "files": [
 * "/livy/hive-site.xml"
 * ]
 * }'
 */
public class SparkJobDO implements Serializable {

    private String file;

    private String className;

    private List<String> args;

    private String name;

    private String queue;

    private Long numExecutors;

    private Long executorCores;

    private String driverMemory;

    private String executorMemory;

    private Map<String, String> conf;

    private List<String> jars;

    private List<String> files;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Long getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(Long numExecutors) {
        this.numExecutors = numExecutors;
    }

    public Long getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(Long executorCores) {
        this.executorCores = executorCores;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public List<String> getJars() {
        return jars;
    }

    public void setJars(List<String> jars) {
        this.jars = jars;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

}
