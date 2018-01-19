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

# Apache Griffin API Guide

This page lists the major RESTful APIs provided by Griffin.

Apache Griffin default `BASE_PATH` is `http://<your ip>:8080`. 

- [Griffin Basic](#1)

- [Measures](#2)

- [Jobs](#3)

- [Metrics](#4)

- [Hive MetaStore](#5)

- [Auth](#6)


<h2 id = "1"></h2>
## Griffin Basic

### Get griffin version
`GET /api/v1/version`

#### Response Body Sample
`0.1.0`

<h2 id = "2"></h2>
## Measures
### Add measure
`POST /api/v1/measures`

#### Request Header
| key          | value            |
| ------------ | ---------------- |
| Content-Type | application/json |

#### Request Body

| name    | description    | type    |
| ------- | -------------- | ------- |
| measure | measure entity | Measure |

There are two different measures that are griffin measure and external measure.
If you want to create an external measure,you can use following example json in request body.
```
{
    "type": "external",
    "name": "external_name",
    "description": " test measure",
    "organization": "orgName",
    "owner": "test",
    "metricName": "metricName"
}
```
Here gives a griffin measure example in request body and response body. 
#### Request Body example 
```
{
    "name":"measure_name",
	"type":"griffin",
    "description":"create a measure",
    "evaluate.rule":{
        "rules":[
            {
                "rule":"source.desc=target.desc",
                "dsl.type":"griffin-dsl",
                "dq.type":"accuracy",
                "details":{}
            }
        ]
    },
    "data.sources":[
        {
            "name":"source",
            "connectors":[
                {
					"name":"connector_name_source",
                    "type":"HIVE",
                    "version":"1.2",
					"data.unit":"1h",
                    "config":{
                        "database":"default",
                        "table.name":"demo_src",
                        "where":"dt=#YYYYMMdd# AND hour=#HH#"
                    },
                    "predicates":[
                        {
                            "type":"file.exist",
                            "config":{
                                "root.path":"hdfs:///griffin/demo_src",
                                "path":"/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ]
                }
            ]
        },
        {
            "name":"target",
            "connectors":[
                {
					"name":"connector_name_target",
                    "type":"HIVE",
                    "version":"1.2",
					"data.unit":"1h",
                    "config":{
                        "database":"default",
                        "table.name":"demo_src",
                        "where":"dt=#YYYYMMdd# AND hour=#HH#"
                    },
                    "predicates":[
                        {
                            "type":"file.exist",
                            "config":{
                                "root.path":"hdfs:///griffin/demo_src",
                                "path":"/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ]
                }
            ]
        }
    ]
}
```
#### Response Body Sample
```
{
  "code": 201,
  "description": "Create Measure Succeed"
}
```
It may return failed messages.Such as,
```
{
  "code": 410,
  "description": "Create Measure Failed, duplicate records"
}

```
The reason for failure may be that connector names already exist or connector names are empty.


### Get measures
`GET /api/v1/measures`
#### Response Body Sample
```
[
    {
        "id": 1,
        "name": "measurename",
        "description": "This is measure test.",
        "owner": "test",
        "deleted": false,
        "process.type": "batch",
        "evaluateRule": {
            "id": 1,
            "rules": [
                {
                    "id": 1,
                    "rule": "source.id=target.id AND source.age=target.age",
                    "dsl.type": "griffin-dsl",
                    "dq.type": "accuracy"
                }
            ]
        },
        "data.sources": [
            {
                "id": 1,
                "name": "source",
                "connectors": [
                    {
                        "id": 1,
                        "type": "HIVE",
                        "version": "1.2",
                        "config": {
                            "database": "default",
                            "table.name": "demo_src"
                        }
                    }
                ]
            },
            {
                "id": 2,
                "name": "target",
                "connectors": [
                    {
                        "id": 2,
                        "type": "HIVE",
                        "version": "1.2",
                        "config": {
                            "database": "default",
                            "table.name": "demo_tgt"
                        }
                    }
                ]
            }
        ]
    }
]
```


### Update measure
`PUT /api/v1/measures`
#### Request Header
| key          | value            |
| ------------ | ---------------- |
| Content-Type | application/json |

#### Request Body
| name    | description    | type    |
| ------- | -------------- | ------- |
| measure | measure entity | Measure |
There are two different measures that are griffin measure and external measure.
If you want to update an external measure,you can use following example json in request body.
```
{
	"id":1,
    "type": "external",
    "name": "external_name",
    "description": " update test measure",
    "organization": "orgName",
    "owner": "test",
    "metricName": "metricName"
}
```
Here gives a griffin measure example in request body and response body. 
#### Request Body example 
```
{
        "id": 1,
        "name": "measure_official_update",
        "description": "create a measure",
        "owner": "test",
        "deleted": false,
        "type": "griffin",
        "process.type": "batch",
        "data.sources": [
            {
                "id": 1,
                "name": "source",
                "connectors": [
                    {
                        "id": 1,
                        "name": "connector_name_source",
                        "type": "HIVE",
                        "version": "1.2",
                        "predicates": [],
                        "data.unit": "1h",
                        "config": {
                            "database": "default",
                            "table.name": "demo_src",
                            "where": "dt=#YYYYMMdd# AND hour=#HH#"
                        }
                    }
                ]
            },
            {
                "id": 2,
                "name": "target",
                "connectors": [
                    {
                        "id": 2,
                        "name": "connector_name_target",
                        "type": "HIVE",
                        "version": "1.2",
                        "predicates": [],
                        "data.unit": "1h",
                        "config": {
                            "database": "default",
                            "table.name": "demo_src",
                            "where": "dt=#YYYYMMdd# AND hour=#HH#"
                        }
                    }
                ]
            }
        ],
        "evaluate.rule": {
            "id": 1,
            "rules": [
                {
                    "id": 1,
                    "rule": "source.desc=target.desc",
                    "dsl.type": "griffin-dsl",
                    "dq.type": "accuracy",
                    "details": {}
                }
            ]
        }
    }
```
#### Response Body Sample
```
{
  "code": 204,
  "description": "Update Measure Succeed"
}
```
It may return failed messages.Such as,
```
{
  "code": 400,
  "description": "Resource Not Found"
}

```

The reason for failure may be that measure id doesn't exist.

### Delete measure
`DELETE /api/v1/measures/{id}`
When deleting a measure,api will also delete related jobs.
#### Path Variable
- id -`required` `Long` measure id

#### Request Sample

`/api/v1/measures/1`

#### Response Body Sample
```
{
  "code": 202,
  "description": "Delete Measures By Id Succeed"
}
```

It may return failed messages.Such as,

```
{
  "code": 400,
  "description": "Resource Not Found"
}

```

The reason for failure may be that measure id doesn't exist.


### Get measure by id
`GET /api/v1/measures/{id}`
#### Path Variable
- id -`required` `Long` measure id

#### Request Sample

`/api/v1/measures/1`

#### Response Body Sample
```
{
    "id": 1,
    "name": "measureName",
    "description": "This is a test measure",
    "organization": "orgName",
    "evaluateRule": {
        "id": 1,
        "rules": [
            {
                "id": 1,
                "rule": "source.id = target.id and source.age = target.age and source.desc = target.desc",
                "dsl.type": "griffin-dsl",
                "dq.type": "accuracy"
            }
        ]
    },
    "owner": "test",
    "deleted": false,
    "process.type": "batch",
    "data.sources": [
        {
            "id": 39,
            "name": "source",
            "connectors": [
                {
                    "id": 1,
                    "type": "HIVE",
                    "version": "1.2",
                    "config": {
                        "database": "default",
                        "table.name": "demo_src"
                    }
                }
            ]
        },
        {
            "id": 2,
            "name": "target",
            "connectors": [
                {
                    "id": 2,
                    "type": "HIVE",
                    "version": "1.2",
                    "config": {
                        "database": "default",
                        "table.name": "demo_tgt"
                    }
                }
            ]
        }
    ]
}
```
It may return no content.That's because your measure id doesn't exist.

<h2 id = "3"></h2>
## Jobs
### Add job
`POST /api/v1/jobs`
#### Request Header
| key          | value            |
| ------------ | ---------------- |
| Content-Type | application/json |

#### Request Body
| name        | description                              | type        |
| ----------- | ---------------------------------------- | ----------- |
| jobSchedule | custom class composed of job key parameters | JobSchedule |

#### Request Body Sample
```
{
    "measure.id": 1,
	"job.name":"job_name",
    "cron.expression": "0 0/4 * * * ?",
    "cron.time.zone": "GMT+8:00",
    "predicate.config": {
		"checkdonefile.schedule":{
			"interval": "5m",
			"repeat": 12
		}
    },
    "data.segments": [
        {
            "data.connector.name": "connector_name_source_test",
			"as.baseline":true, 
            "segment.range": {
                "begin": "-1h",
                "length": "1h"
            }
        },
        {
            "data.connector.name": "connector_name_target_test",
            "segment.range": {
                "begin": "-1h",
                "length": "1h"
            }
        }
    ]
}
```
#### Response Body Sample
```
{
  "code": 205,
  "description": "Create Job Succeed"
}
```
It may return failed messages.Such as,

```
{
  "code": 405,
  "description": "Create Job Failed"
}
```

There are several reasons to create job failure.

- Measure id does not exist.
- Job name already exits.
- Param as.baselines aren't set or are all false.
- Connector name doesn't exist in your measure.
- The trigger key already exists.

### Get jobs
`GET /api/v1/jobs`

#### Response Body Sample
```
[
    {
        "jobId": 1,
        "jobName": "job_name",
        "measureId": 1,
        "triggerState": "NORMAL",
        "nextFireTime": 1515400080000,
        "previousFireTime": 1515399840000,
        "cronExpression": "0 0/4 * * * ?"
    }
]

```

### Delete job by id
#### `DELETE /api/v1/jobs/{id}`
#### Path Variable
- id -`required` `Long` job id

#### Response Body Sample
```
{
  "code": 206,
  "description": "Delete Job Succeed"
}

```
It may return failed messages.Such as,
```
{
    "code": 406,
    "description": "Delete Job Failed"
}
```
The reason for failure may be that job id does not exist.

### Delete job by name
#### `DELETE /api/v1/jobs`
| name    | description | type   | example value |
| ------- | ----------- | ------ | ------------- |
| jobName | job name    | String | job_name      |

#### Response Body Sample
```
{
  "code": 206,
  "description": "Delete Job Succeed"
}

```
It may return failed messages.Such as,
```
{
    "code": 406,
    "description": "Delete Job Failed"
}
```
The reason for failure may that job name does not exist.


### Get job instances
`GET /api/v1/jobs/instances`

| name  | description                         | type | example value |
| ----- | ----------------------------------- | ---- | ------------- |
| jobId | job id                              | Long | 1             |
| page  | page you want starting from index 0 | int  | 0             |
| size  | instance number per page            | int  | 10            |

#### Response Body Sample
```
[
    {
        "id": 1,
        "sessionId": null,
        "state": "success",
        "appId": null,
        "appUri": null,
        "predicateGroup": "PG",
        "predicateName": "job_name_predicate_1515399840077",
        "deleted": true,
        "timestamp": 1515399840092,
        "expireTimestamp": 1516004640092
    },
    {
        "id": 2,
        "sessionId": null,
        "state": "not_found",
        "appId": null,
        "appUri": null,
        "predicateGroup": "PG",
        "predicateName": "job_name_predicate_1515399840066",
        "deleted": true,
        "timestamp": 1515399840067,
        "expireTimestamp": 1516004640067
    }
]
```

### Get job healthy statistics
`GET /api/v1/jobs/health`

#### Response Body Sample
```
{
  "healthyJobCount": 1,
  "jobCount": 2
}
```

<h2 id = "4"></h2>
## Metrics
### Get metrics
`GET /api/v1/metrics`
#### Response Example
```
[
    {
        "name": "external_name",
        "description": " test measure",
        "organization": "orgName",
        "owner": "test",
        "metricValues": [
            {
                "name": "metricName",
                "tmst": 1509599811123,
                "value": {
                    "__tmst": 1509599811123,
                    "miss": 11,
                    "total": 125000,
                    "matched": 124989
                }
            }
        ]
    }
]
```

### Add metric values
`POST /api/v1/metrics/values`
#### Request Header
| key          | value            |
| ------------ | ---------------- |
| Content-Type | application/json |
#### Request Body
| name          | description             | type        |
| ------------- | ----------------------- | ----------- |
| Metric Values | A list of metric values | MetricValue |
#### Request Body Sample
```
[
	{
		"name" : "metricName",
		"tmst" : 1509599811123,
		"value" : {
			"__tmst" : 1509599811123,
			"miss" : 11,
			"total" : 125000,
			"matched" : 124989
		}
   }
]
```
#### Response Body Sample
```
{
    "code": 210,
    "description": "Add Metric Values Success"
}
```

It may return failed message

```
{
	"code": 412,
    "description": "Add Metric Values Failed"
}
```
The returned HTTP status code identifies the reason for failure.
### Get metric values by name 
`GET /api/v1/metrics/values`

#### Request Parameter
| name       | description                              | type   | example value |
| ---------- | ---------------------------------------- | ------ | ------------- |
| metricName | name of the metric values                | String | metricName    |
| size       | max amount of return values              | int    | 5             |
| offset     | the amount of records to skip by timestamp in descending order | int    | 0             |

Parameter offset is optional, it has default value as 0.
#### Response Body Sample
```
[
    {
        "name": "metricName",
        "tmst": 1509599811123,
        "value": {
            "__tmst": 1509599811123,
            "miss": 11,
            "total": 125000,
            "matched": 124989
        }
    }
]
```

### Delete metric values by name
`DELETE /api/v1/metrics/values`
#### Request Parameters 
| name       | description               | type   | example value |
| ---------- | ------------------------- | ------ | ------------- |
| metricName | name of the metric values | String | metricName    |
#### Response Body Sample
```
{
    "code": 211,
    "description": "Delete Metric Values Success"
}
```
It may return failed messages
```
{
    "code": 413,
    "description": "Delete Metric Values Failed"
}
```
The returned HTTP status code identifies the reason for failure.

<h2 id = "5"></h2>
### Hive MetaStore
### Get table metadata
`GET /api/v1/metadata/hive/table`
#### Request Parameters
| name  | description        | type   | example value |
| ----- | ------------------ | ------ | ------------- |
| db    | hive database name | String | default       |
| table | hive table name    | String | demo_src      |

#### Response Example Sample
```
{
    "tableName": "demo_src",
    "dbName": "default",
    "owner": "root",
    "createTime": 1505986176,
    "lastAccessTime": 0,
    "retention": 0,
    "sd": {
        "cols": [
            {
                "name": "id",
                "type": "bigint",
                "comment": null,
                "setName": true,
                "setType": true,
                "setComment": false
            },
            {
                "name": "age",
                "type": "int",
                "comment": null,
                "setName": true,
                "setType": true,
                "setComment": false
            },
            {
                "name": "desc",
                "type": "string",
                "comment": null,
                "setName": true,
                "setType": true,
                "setComment": false
            }
        ],
        "location": "hdfs://sandbox:9000/griffin/data/batch/demo_src"
    },
    "partitionKeys": [
        {
            "name": "dt",
            "type": "string",
            "comment": null,
            "setName": true,
            "setType": true,
            "setComment": false
        },
        {
            "name": "hour",
            "type": "string",
            "comment": null,
            "setName": true,
            "setType": true,
            "setComment": false
        }
    ]
}
```
### Get table names
`GET /api/v1/metadata/hive/tables/names`
#### Request Parameter
| name | description        | typ    | example value |
| ---- | ------------------ | ------ | ------------- |
| db   | hive database name | String | default       |

#### Response Example Sample
```
[
  "demo_src",
  "demo_tgt"
]
```

### Get all database tables metadata
`GET /api/v1/metadata/hive/dbs/tables`
#### Response Example Sample
```
{
   "default": [
    {
      "tableName": "demo_src",
      "dbName": "default",
      "owner": "root",
      "createTime": 1505986176,
      "lastAccessTime": 0,
      "sd": {
        "cols": [
          {
            "name": "id",
            "type": "bigint",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          },
          {
            "name": "age",
            "type": "int",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          },
          {
            "name": "desc",
            "type": "string",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          }
        ],
        "location": "hdfs://sandbox:9000/griffin/data/batch/demo_src"
      },
      "partitionKeys": [
        {
          "name": "dt",
          "type": "string",
          "comment": null,
          "setComment": false,
          "setType": true,
          "setName": true
        },
        {
          "name": "hour",
          "type": "string",
          "comment": null,
          "setComment": false,
          "setType": true,
          "setName": true
        }
      ]
    },
    {
      "tableName": "demo_tgt",
      "dbName": "default",
      "owner": "root",
      "createTime": 1505986176,
      "lastAccessTime": 0,
      "sd": {
        "cols": [
          {
            "name": "id",
            "type": "bigint",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          },
          {
            "name": "age",
            "type": "int",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          },
          {
            "name": "desc",
            "type": "string",
            "comment": null,
            "setComment": false,
            "setType": true,
            "setName": true
          }
        ],
        "location": "hdfs://sandbox:9000/griffin/data/batch/demo_tgt"
      },
      "partitionKeys": [
        {
          "name": "dt",
          "type": "string",
          "comment": null,
          "setComment": false,
          "setType": true,
          "setName": true
        },
        {
          "name": "hour",
          "type": "string",
          "comment": null,
          "setComment": false,
          "setType": true,
          "setName": true
        }
      ]
    }
  ]
}

```

### Get database names
`GET /api/v1/metadata/hive/dbs`
#### Response Example Sample
```
[
	"default"
]
```

### Get tables metadata
`GET /api/v1/metadata/hive/tables`
#### Request Parameter
| name | description        | typ    | example value |
| ---- | ------------------ | ------ | ------------- |
| db   | hive database name | String | default       |
#### Response Body Sample
```
[
  {
    "tableName": "demo_src",
    "dbName": "default",
    "owner": "root",
    "createTime": 1508216660,
    "lastAccessTime": 0,
    "retention": 0,
    "sd": {
      "cols": [
        {
          "name": "id",
          "type": "bigint",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        },
        {
          "name": "age",
          "type": "int",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        },
        {
          "name": "desc",
          "type": "string",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        }
      ],
      "location": "hdfs://sandbox:9000/griffin/data/batch/demo_src"
    },
    "partitionKeys": [
      {
        "name": "dt",
        "type": "string",
        "comment": null,
        "setName": true,
        "setType": true,
        "setComment": false
      },
      {
        "name": "hour",
        "type": "string",
        "comment": null,
        "setName": true,
        "setType": true,
        "setComment": false
      }
    ]
  },
  {
    "tableName": "demo_tgt",
    "dbName": "default",
    "owner": "root",
    "createTime": 1508216660,
    "lastAccessTime": 0,
    "retention": 0,
    "sd": {
      "cols": [
        {
          "name": "id",
          "type": "bigint",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        },
        {
          "name": "age",
          "type": "int",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        },
        {
          "name": "desc",
          "type": "string",
          "comment": null,
          "setName": true,
          "setType": true,
          "setComment": false
        }
      ],
      "location": "hdfs://sandbox:9000/griffin/data/batch/demo_tgt"
 },
    "partitionKeys": [
      {
        "name": "dt",
        "type": "string",
        "comment": null,
        "setName": true,
        "setType": true,
        "setComment": false
      },
      {
        "name": "hour",
        "type": "string",
        "comment": null,
        "setName": true,
        "setType": true,
        "setComment": false
      }
    ]
  }
]
```


<h2 id = "6"></h2>
## Auth
### User authentication
`POST /api/v1/login/authenticate`

#### Request Parameter
| name | description                           | type | example value                           |
| ---- | ------------------------------------- | ---- | --------------------------------------- |
| map  | a map contains user name and password | Map  | `{"username":"user","password":"test"}` |

#### Response Body Sample
```
{
  "fullName": "Default",
  "ntAccount": "user",
  "status": 0
}
```
