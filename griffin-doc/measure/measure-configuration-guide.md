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

# Apache Griffin Measure Configuration Guide
Apache Griffin measure module needs two configuration files to define the parameters of execution, one is for environment, the other is for dq job.

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

  "sinks": [
    {
      "type": "console",
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

  "griffin.checkpoint": [
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
- **sinks**: This field configures list of metrics sink parameters, multiple sink ways are supported. Details of sink configuration [here](#sinks).
- **griffin.checkpoint**: This field configures list of griffin checkpoint parameters, multiple cache ways are supported. It is only for streaming dq case. Details of info cache configuration [here](#griffin-checkpoint).

### <a name="sinks"></a>Sinks
- **type**: Metrics and records sink type, "console", "hdfs", "http", "mongo". 
- **config**: Configure parameters of each sink type.
	+ console sink (aliases: "log")
		* max.log.lines: the max lines of log.
	+ hdfs sink
		* path: hdfs path to sink metrics
		* max.persist.lines: the max lines of total sink data.
		* max.lines.per.file: the max lines of each sink file.
	+ http sink (aliases: "es", "elasticsearch")
		* api: api to submit sink metrics.
		* method: http method, "post" default.
    + mongo sink
        * url: url of mongo db.
        * database: database name.
        * collection: collection name. 

### <a name="griffin-checkpoint"></a>Griffin Checkpoint
- **type**: Griffin checkpoint type, "zk" for zookeeper checkpoint.
- **config**: Configure parameters of griffin checkpoint type.
	+ zookeeper checkpoint
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

  "process.type": "BATCH",

  "data.sources": [
    {
      "name": "src",
      "connectors": [
        {
          "type": "AVRO",
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
          "type": "AVRO",
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
        "dq.type": "ACCURACY",
        "out.dataframe.name": "accu",
        "rule": "source.user_id = target.user_id AND upper(source.first_name) = upper(target.first_name) AND source.last_name = target.last_name AND source.address = target.address AND source.email = target.email AND source.phone = target.phone AND source.post_code = target.post_code",
        "details": {
          "source": "source",
          "target": "target",
          "miss": "miss_count",
          "total": "total_count",
          "matched": "matched_count"
        },        
        "out": [
          {
            "type": "metric",
            "name": "accu"
          },
          {
            "type": "record"
          }        
        ]
      }
    ]
  },
  
  "sinks": ["CONSOLE", "HTTP", "HDFS"]
}
```
Above lists DQ job configure parameters.  

- **name**: Name of DQ job.
- **process.type**: Process type of DQ job, "BATCH" or "STREAMING".
- **data.sources**: List of data sources in this DQ job.
	+ name: Name of this data source, it should be different from other data sources.
	+ connectors: List of data connectors combined as the same data source. Details of data connector configuration [here](#data-connector).
- **evaluate.rule**: Evaluate rule parameters of this DQ job.
	+ dsl.type: Default dsl type of all the rules.
	+ rules: List of rules, to define every rule step. Details of rule configuration [here](#rule).
- **sinks**: Whitelisted sink types for this job. Note: no sinks will be used, if empty or omitted. 

### <a name="data-connector"></a>Data Connector
- **type**: Data connector type: "AVRO", "HIVE", "TEXT-DIR", "CUSTOM" for batch mode; "KAFKA", "CUSTOM" for streaming mode.
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
	+ custom connector
	    * class: class name for user-provided data connector implementation. For Batch
	    it should be implementing BatchDataConnector trait and have static method with signature
	    ```def apply(ctx: BatchDataConnectorContext): BatchDataConnector```. 
	    For Streaming, it should be implementing StreamingDataConnector and have static method
	    ```def apply(ctx: StreamingDataConnectorContext): StreamingDataConnector```. User-provided
	    data connector should be present in Spark job's class path, by providing custom jar as -jar parameter
	    to spark-submit or by adding to "jars" list in sparkProperties.json.  

### <a name="rule"></a>Rule
- **dsl.type**: Rule dsl type, "spark-sql", "df-ops" and "griffin-dsl".
- **dq.type**: DQ type of this rule, only for "griffin-dsl" type. Supported types: "ACCURACY", "PROFILING", "TIMELINESS", "UNIQUENESS", "COMPLETENESS".
- **out.dataframe.name** (step information): Output table name of this rule, could be used in the following rules.
- **in.dataframe.name** (step information): Input table name of this rule, only used for "df-ops" type.
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
- **cache**: Cache output dataframe. Optional, valid only for "spark-sql" and "df-ops" mode. Defaults to `false` if not specified.
- **out**: List of output sinks for the job.
  + Metric output.
    * type: "metric"
    * name: Metric name, semantics depends on "flatten" field value.   
    * flatten: Aggregation method used before sending data frame result into the sink:  
      - default: use "array" if data frame returned multiple records, otherwise use "entries" 
      - entries: sends first row of data frame as metric results, like like `{"agg_col": "value"}`
      - array: wraps all metrics into a map, like `{"my_out_name": [{"agg_col": "value"}]}`
      - map: wraps first row of data frame into a map, like `{"my_out_name": {"agg_col": "value"}}`
  + Record output. Currently handled only by HDFS sink.
    * type: "record"
    * name: File name within sink output folder to dump files to.   
  + Data source cache update for streaming jobs.
    * type: "dsc-update"
    * name: Data source name to update cache.   
