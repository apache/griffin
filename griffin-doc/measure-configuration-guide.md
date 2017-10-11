<!--
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
-->

# Griffin Measure Configuration Guide
Griffin measure module needs two configuration files to define the parameters of execution, one is for environment, the other is for dq job.

## Environment Parameters
```
{
  "spark": {
    "log.level": "WARN",
    "checkpoint.dir": "hdfs:///griffin/streaming/cp",
    "batch.interval": "5s",
    "process.interval": "30s",
    "config": {
      "spark.task.maxFailures": 5,
      "spark.streaming.kafkaMaxRatePerPartition": 1000,
      "spark.streaming.concurrentJobs": 4
    }
  },

  "persist": [
    {
      "type": "log",
      "config": {
        "max.log.lines": 100
      }
    }, {
      "type": "hdfs",
      "config": {
        "path": "hdfs:///griffin/streaming/persist",
        "max.persist.lines": 10000,
        "max.lines.per.file": 10000
      }
    }
  ],

  "info.cache": [
    {
      "type": "zk",
      "config": {
        "hosts": "<zookeeper host ip>:2181",
        "namespace": "griffin/infocache",
        "lock.path": "lock",
        "mode": "persist",
        "init.clear": true,
        "close.clear": false
      }
    }
  ]
}
```
Above lists environment parameters.

- **spark**: This field configures spark and spark streaming parameters.  
- **persist**: This field configures the metrics persist parameters, multiple persist ways are supported.
- **info.cache**: This field configures the information cache parameters, multiple cache ways are supported. It is only for streaming dq case.



## DQ Job Parameters
