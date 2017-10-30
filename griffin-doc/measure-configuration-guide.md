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
- **persist**: This field configures list of metrics persist parameters, multiple persist ways are supported. Details of persist configuration [here](#persist).
- **info.cache**: This field configures list of information cache parameters, multiple cache ways are supported. It is only for streaming dq case. Details of info cache configuration [here](#info-cache).

### <a name="persist"></a>Persist
- **type**: Metrics persist type, "log", "hdfs" and "http". 
- **config**: Configure parameters of each persist type.
	+ log persist
		* max.log.lines: the max lines of log.
	+ hdfs persist
		* path: hdfs path to persist metrics
		* max.persist.lines: the max lines of total persist data.
		* max.lines.per.file: the max lines of each persist file.
	+ http persist
		* api: api to submit persist metrics.
		* method: http method, "post" default.

### <a name="info-cache"></a>Info Cache
- **type**: Information cache type, "zk" for zookeeper cache.
- **config**: Configure parameters of info cache type.
	+ zookeeper cache
		* hosts: zookeeper hosts list as a string, separated by comma.
		* namespace: namespace of cache info, "" as default.
		* lock.path: path of lock info, "lock" as default.
		* mode: create mode of zookeeper node, "persist" as default.
		* init.clear: clear cache info when initialize, true default.
		* close.clear: clear cache info when close connection, false default.

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
          	"file.path": "<path>/<to>",
            "file.name": "<source-file>.avro"
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
          	"file.path": "<path>/<to>",
            "file.name": "<target-file>.avro"
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

- **name**: Name of DQ job.
- **process.type**: Process type of DQ job, "batch" or "streaming".
- **data.sources**: List of data sources in this DQ job.
	+ name: Name of this data source, it should be different from other data sources.
	+ connectors: List of data connectors combined as the same data source. Details of data connector configuration [here](#data-connector).
- **evaluateRule**: Evaluate rule parameters of this DQ job.
	+ dsl.type: Default dsl type of all the rules.
	+ rules: List of rules, to define every rule step. Details of rule configuration [here](#rule).

### <a name="data-connector"></a>Data Connector
- **type**: Data connector type, "avro", "hive", "text-dir" for batch mode, "kafka" for streaming mode.
- **version**: Version string of data connector type.
- **config**: Configure parameters of each data connector type.
	+ avro data connector
		* file.path: avro file path, optional, "" as default.
		* file.name: avro file name.
	+ hive data connector
		* database: data base name, optional, "default" as default.
		* table.name: table name.
		* partitions: partition conditions string, split by ";" and ",", optional. 
			e.g. `dt=20170410, hour=15; dt=20170411, hour=15; dt=20170412, hour=15`
	+ text dir data connector
		* dir.path: parent directory path.
		* data.dir.depth: integer, depth of data directories, 0 as default.
		* success.file: success file name, 
		* done.file: 

### <a name="rule"></a>Rule
- **dsl.type**: Rule dsl type, "spark-sql", "df-opr" and "griffin-dsl".
- **name** (step information): Result table name of this rule, optional for "griffin-dsl" type.
- **persist.type** (step information): Persist type of result table, optional for "griffin-dsl" type. Supporting "metric", "record" and "none" type, "metric" type indicates the result will be persisted as metrics, "record" type indicates the result will be persisted as record only, "none" type indicates the result will not be persisted. Default is "none" type.
- **update.data.source** (step information): If the result table needs to update the data source, this parameter is the data source name, for streaming accuracy case, optional.
- **dq.type**: DQ type of this rule, only for "griffin-dsl" type, supporting "accuracy" and "profiling".
- **details**: Details of this rule, optional.
	+ accuracy dq type detail configuration
		* source: the data source name which as source in accuracy, default is the name of first data source in "data.sources" if not configured.
		* target: the data source name which as target in accuracy, default is the name of second data source in "data.sources" if not configured.
		* miss.records: step information of miss records result table step in accuracy.
		* accuracy: step information of accuracy result table step in accuracy.
		* miss: alias of miss column in result table.
		* total: alias of total column in result table.
		* matched: alias of matched column in result table.
	+ profiling dq type detail configuration
		* source: the data source name which as source in profiling, default is the name of first data source in "data.sources" if not configured. If the griffin-dsl rule contains from clause, this parameter is ignored.
		* profiling: step information of profiling result table step in profiling.