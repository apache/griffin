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

# Measure Batch Sample
Measures consists of batch measure and streaming measure. This document is for the batch measure sample.

## Batch Accuracy Sample
```
{
  "name": "accu_batch",

  "process.type": "batch",

  "data.sources": [
    {
      "name": "source",
      "baseline": true,
      "connectors": [
        {
          "type": "avro",
          "version": "1.7",
          "config": {
            "file.name": "src/test/resources/users_info_src.avro"
          }
        }
      ]
    }, {
      "name": "target",
      "connectors": [
        {
          "type": "avro",
          "version": "1.7",
          "config": {
            "file.name": "src/test/resources/users_info_target.avro"
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
Above is the configure file of batch accuracy job.  

### Data source
In this sample, we use avro file as source and target.  

### Evaluate rule
In this accuracy sample, the rule describes the match condition: `src.user_id = tgt.user_id AND upper(src.first_name) = upper(tgt.first_name) AND src.last_name = tgt.last_name`.  
The accuracy metrics will be persisted as metric, with miss column named "miss_count", total column named "total_count", matched column named "matched_count".  
The miss records of source will be persisted as record.  

## Batch Profiling Sample
```
{
  "name": "prof_batch",

  "process.type": "batch",

  "data.sources": [
    {
      "name": "source",
      "connectors": [
        {
          "type": "avro",
          "version": "1.7",
          "config": {
            "file.name": "src/test/resources/users_info_src.avro"
          }
        }
      ]
    }
  ],

  "evaluate.rule": {
    "rules": [
      {
        "dsl.type": "griffin-dsl",
        "dq.type": "profiling",
        "name": "prof",
        "rule": "select count(*) as `cnt`, count(distinct `post_code`) as `dis-cnt`, max(user_id) as `max` from source",
        "metric": {
          "name": "prof"
        }
      },
      {
        "dsl.type": "griffin-dsl",
        "dq.type": "profiling",
        "name": "grp",
        "rule": "select post_code as `pc`, count(*) as `cnt` from source group by post_code",
        "metric": {
          "name": "post_group",
          "collect.type": "array"
        }
      }
    ]
  }
}
```
Above is the configure file of batch profiling job.  

### Data source
In this sample, we use hive table as source.  

### Evaluate rule
In this profiling sample, the rule describes the profiling request: `country, country.count() as cnt group by country order by cnt desc limit 3`.  
The profiling metrics will be persisted as metric, listing the most 3 groups of items in same country.  