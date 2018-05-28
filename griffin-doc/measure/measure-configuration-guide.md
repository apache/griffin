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
    "batch.interval": "1m",
    "process.interval": "5m",
    "config": {
      "spark.default.parallelism": 5,
      "spark.task.maxFailures": 5,
      "spark.streaming.kafkaMaxRatePerPartition": 1000,
      "spark.streaming.concurrentJobs": 4,
      "spark.yarn.maxAppAttempts": 5,
      "spark.yarn.am.attemptFailuresValidityInterval": "1h",
      "spark.yarn.max.executor.failures": 120,
      "spark.yarn.executor.failuresValidityInterval": "1h",
      "spark.hadoop.fs.hdfs.impl.disable.cache": true
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
  + mongo persist
    * url: url of mongo db.
    * database: database name.
    * collection: collection name. 

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

  "evaluate.rule": {
    "rules": [
      {
        "dsl.type": "griffin-dsl",
        "dq.type": "accuracy",
        "name": "accu",
        "rule": "source.user_id = target.user_id AND upper(source.first_name) = upper(target.first_name) AND source.last_name = target.last_name AND source.address = target.address AND source.email = target.email AND source.phone = target.phone AND source.post_code = target.post_code",
        "details": {
          "source": "source",
          "target": "target",
          "miss": "miss_count",
          "total": "total_count",
          "matched": "matched_count"
        },
        "metric": {
          "name": "accu"
        },
        "record": {
          "name": "missRecords"
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
		* where: where conditions string, split by ",", optional.
			e.g. `dt=20170410 AND hour=15, dt=20170411 AND hour=15, dt=20170412 AND hour=15`
	+ text dir data connector
		* dir.path: parent directory path.
		* data.dir.depth: integer, depth of data directories, 0 as default.
		* success.file: success file name, 
		* done.file: 

### <a name="rule"></a>Rule
- **dsl.type**: Rule dsl type, "spark-sql", "df-opr" and "griffin-dsl".
- **dq.type**: DQ type of this rule, only for "griffin-dsl" type, supporting "accuracy" and "profiling".
- **name** (step information): Result table name of this rule, optional for "griffin-dsl" type.
- **rule**: The rule string.
- **details**: Details of this rule, optional.
  + accuracy dq type detail configuration
    * source: the data source name which as source in accuracy, default is the name of first data source in "data.sources" if not configured.
    * target: the data source name which as target in accuracy, default is the name of second data source in "data.sources" if not configured.
    * miss: the miss count name in metric, optional.
    * total: the total count name in metric, optional.
    * matched: the matched count name in metric, optional.
  + profiling dq type detail configuration
    * source: the data source name which as source in profiling, default is the name of first data source in "data.sources" if not configured. If the griffin-dsl rule contains from clause, this parameter is ignored.
  + uniqueness dq type detail configuration
    * source: name of data source to measure uniqueness.
    * target: name of data source to compare with. It is always the same as source, or more than source.
    * unique: the unique count name in metric, optional.
    * total: the total count name in metric, optional.
    * dup: the duplicate count name in metric, optional.
    * num: the duplicate number name in metric, optional.
    * duplication.array: optional, if set as a non-empty string, the duplication metric will be computed, and the group metric name is this string.
  + distinctness dq type detail configuration
    * source: name of data source to measure uniqueness.
    * target: name of data source to compare with. It is always the same as source, or more than source.
    * distinct: the unique count name in metric, optional.
    * total: the total count name in metric, optional.
    * dup: the duplicate count name in metric, optional.
    * accu_dup: the accumulate duplicate count name in metric, optional, only in streaming mode and "with.accumulate" enabled.
    * num: the duplicate number name in metric, optional.
    * duplication.array: optional, if set as a non-empty string, the duplication metric will be computed, and the group metric name is this string.
    * with.accumulate: optional, default is true, if set as false, in streaming mode, the data set will not compare with old data to check distinctness.
  + timeliness dq type detail configuration
    * source: name of data source to measure timeliness.
    * latency: the latency column name in metric, optional.
    * threshold: optional, if set as a time string like "1h", the items with latency more than 1 hour will be record.
- **metric**: Configuration of metric export.
  + name: name of metric.
  + collect.type: collect metric as the type set, including "default", "entries", "array", "map", optional.
- **record**: Configuration of record export.
  + name: name of record.
  + data.source.cache: optional, if set as data source name, the cache of this data source will be updated by the records, always used in streaming accuracy case.
  + origin.DF: avaiable only if "data.source.cache" is set, the origin data frame name of records.