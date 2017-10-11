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
# Measure streaming sample
Measures consists of batch measure and streaming measure. This document is for the streaming measure sample.

### Data source
At current, we support kafka as streaming data source.  
In this sample, we also need a kafka as data source.

### Measure type
At current, we support accuracy measure in streaming mode.

### Kafka decoder
In kafka, data always needs encode and decode, we support String type kafka data currently, you can also implement and use your decoder for kafka case.

### Environment
For current griffin streaming case, we need some necessary environment dependencies, zookeeper and hdfs.  
We use zookeeper to cache some checkpoint information, it's optional, but we recommend it.  
We use hdfs to save the temporary data, it's also a recommend selection.

### Streaming accuracy result
The streaming data will be separated into mini-batches of data, for each mini-batch data, there should be an accuracy result. Therefore, the streaming accuracy result should be a bunch of batch accuracy results with timestamp.  
Considering the latency of streaming data, which means the source data and the matching target data will not exactly reach exactly at the same time, we have to accept some delay of data in streaming mode, by holding unmatched data in memory or disk, and try to match them later until the data is out-time.

## How to run streaming sample
### Environment Preparation
At first, we need some environment preparation.  
- Zookeeper: Zookeeper 3.4.10
- Hadoop: Hadoop 2.6
- Spark: Spark 1.6
- Kafka: Kafka 0.8

### Data Preparation
Create two topics in kafka, for source and target data. For example, topic "source" for source data, and topic "target" for target data.  
Streaming data should also be prepared, the format could be json string, for example:  
Source data could be:
```
{"name": "kevin", "age": 24}
{"name": "jason", "age": 25}
{"name": "jhon", "age": 28}
{"name": "steve", "age": 31}
```
Target data could be:
```
{"name": "kevin", "age": 24}
{"name": "jason", "age": 25}
{"name": "steve", "age": 20}
```
You need to input the source data and target data into these two topics, through console producer might be a good choice for experimental purpose.

### Configuration Preparation
Two configuration files are required.
Environment configuration file: env.json
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
In env.json, "spark" field configures the spark and spark streaming parameters, "persist" field configures the persist ways, we support "log", "hdfs" and "http" ways at current, "info.cache" field configures the information cache parameters, we support zookeeper only at current.  

Process configuration file: config.json
```
{
  "name": "streaming-accu-sample",
  "type": "accuracy",
  "process.type": "streaming",

  "source": {
    "type": "kafka",
    "version": "0.8",
    "config": {
      "kafka.config": {
        "bootstrap.servers": "<kafka host ip>:9092",
        "group.id": "group1",
        "auto.offset.reset": "smallest",
        "auto.commit.enable": "false"
      },
      "topics": "source",
      "key.type": "java.lang.String",
      "value.type": "java.lang.String"
    },
    "cache": {
      "type": "text",
      "config": {
        "file.path": "hdfs:///griffin/streaming/dump/source",
        "info.path": "source",
        "ready.time.interval": "10s",
        "ready.time.delay": "0"
      },
      "time.range": ["-5m", "0"]
    },
    "match.once": true
  },

  "target": {
    "type": "kafka",
    "version": "0.8",
    "config": {
      "kafka.config": {
        "bootstrap.servers": "<kafka host ip>:9092",
        "group.id": "group1",
        "auto.offset.reset": "smallest",
        "auto.commit.enable": "false"
      },
      "topics": "target",
      "key.type": "java.lang.String",
      "value.type": "java.lang.String"
    },
    "cache": {
      "type": "text",
      "config": {
        "file.path": "hdfs:///griffin/streaming/dump/target",
        "info.path": "target",
        "ready.time.interval": "10s",
        "ready.time.delay": "0"
      },
      "time.range": ["-5m", "0"]
    },
    "match.once": false
  },

  "evaluateRule": {
    "rules": "$source.json().name = $target.json().name AND $source.json().age = $target.json().age"
  }
}
```
In config.json, "source" and "target" fields configure the data source parameters.  
The "cache" field in data source configuration represents the temporary data cache way, at current we support "text" and "hive" ways. We recommend "text" way, it only depends on hdfs. "time.range" means that the data older than the lower bound should be considered as out-time, and the out-time data will not be calculated any more.   
"match.once" represents the data from this data source could be matched only once or more times.  
"evaluateRule.rule" configures the match rule between each source and target data.

### Run
Build the measure package.
```
mvn clean install
```
Get the measure package ```measure-<version>-incubating-SNAPSHOT.jar```, rename it to ```griffin-measure.jar```.  
Put measure package together with env.json and config.json.
Run the following command:
```
spark-submit --class org.apache.griffin.measure.Application \
--master yarn-client --queue default \
griffin-measure.jar \
env.json config.json local,local
```
The first two parameters are the paths of env.json and config.json, the third parameter represents the file system type of the two configuration files, "local" or "hdfs" are both supported.  

The spark streaming application will be long-time running, you can get the results of each mini-batch of data, during the run-time, you can also input more data into source and target topics, to check the results of the later mini-batches.
