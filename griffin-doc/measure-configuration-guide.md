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
	+ log.level: Level of spark log.
	+ checkpoint.dir: Check point directory of spark streaming, for streaming mode.
	+ batch.interval: Interval of dumping streaming data, for streaming mode.
	+ process.interval: Interval of processing dumped streaming data, for streaming mode.
	+ config: Configuration of spark parameters.
- **persist**: This field configures list of metrics persist parameters, multiple persist ways are supported.
	+ type: Metrics persist type, "log", "hdfs" or "http".
	+ config: Configure parameters of each persist type.
		* log persist
			- max.log.lines: the max lines of log.
		* hdfs persist
			- path: hdfs path to persist metrics
			- max.persist.lines: the max lines of total persist data.
			- max.lines.per.file: the max lines of each persist file.
		* http persist
			- api: api to submit persist metrics.
			- method: http method, "post" default.
- **info.cache**: This field configures list of information cache parameters, multiple cache ways are supported. It is only for streaming dq case.
	+ type: Information cache type, "zk" for zookeeper cache.
	+ config: Configure parameters of info cache type.
		* zookeeper cache
			- hosts: zookeeper hosts list as a string, separated by comma.
			- namespace: namespace of cache info, "" default.
			- lock.path: path of lock info, "lock" default.
			- mode: create mode of zookeeper node, "persist" default.
			- init.clear: clear cache info when initialize, true default.
			- close.clear: clear cache info when close connection, false default.


## DQ Job Parameters
```
{
  "name": "accu_batch",

  "process.type": "batch",

  "data.sources": [
    {
      "name": "src",
      "connectors": [
        {
          "type": "avro",
          "version": "1.7",
          "config": {
            "file.name": "<path>/<to>/<source-file>.avro"
          }
        }
      ]
    }, {
      "name": "tgt",
      "connectors": [
        {
          "type": "avro",
          "version": "1.7",
          "config": {
            "file.name": "<path>/<to>/<target-file>.avro"
          }
        }
      ]
    }
  ],

  "evaluateRule": {
    "rules": [
      {
        "dsl.type": "griffin-dsl",
        "dq.type": "accuracy",
        "rule": "src.user_id = tgt.user_id AND upper(src.first_name) = upper(tgt.first_name) AND src.last_name = tgt.last_name",
        "details": {
          "source": "src",
          "target": "tgt",
          "miss.records": {
            "name": "miss.records",
            "persist.type": "record"
          },
          "accuracy": {
            "name": "accu",
            "persist.type": "metric"
          },
          "miss": "miss_count",
          "total": "total_count",
          "matched": "matched_count"
        }
      }
    ]
  }
}
```
Above lists DQ job configure parameters.  

- 