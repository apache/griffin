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

This page lists the major RESTful APIs provided by Apache Griffin.

Apache Griffin default `BASE_PATH` is `http://<your ip>:8080`. 

- [HTTP Response Design](#0)

- [Griffin Basic](#1)

- [Measures](#2)

- [Jobs](#3)

- [Metrics](#4)

- [Hive MetaStore](#5)

- [Auth](#6)


<h2 id = "0"></h2>

## HTTP Response Design
We follow general rules to design Apache Griffin's REST APIs. In the HTTP response that is sent to a client, 
the status code, which is a three-digit number, is accompanied by a reason phrase (also known as status text) that simply describes the meaning of the code. 
The status codes are classified by number range, with each class of codes having the same basic meaning.
* The range 100-199 is classed as Informational.
* 200-299 is Successful.
* 300-399 is Redirection.
* 400-499 is Client error.
* 500-599 is Server error.

### Valid Apache Griffin Response
The valid HTTP response is designed as follows:

| Action | HTTP Status | Response Body |
| ---- | ------------------ | ------ |
| POST | 201, "Created" | created item |
| GET | 200, "OK" | requested items |
| PUT | 204, "No Content" | no content |
| DELETE | 204, "No Content" | no content |

***Note that:*** The metric module is implemented with elasticsearch bulk api, so the responses do not follow rules above.

### Invalid Apache Griffin Response
The response for exception is designed as follows:

| Action | HTTP Status | Response Body |
| ---- | ------------------ | ------ |
| ANY | 400, "Bad Request" | error detail |
| ANY | 500, "Internal Server Error" | error detail |
```
{
    "timestamp": 1517208444322,
    "status": 400,
    "error": "Bad Request",
    "code": 40009,
    "message": "Property 'measure.id' is invalid",
    "path": "/api/v1/jobs"
}
```
```
{
    "timestamp": 1517209428969,
    "status": 500,
    "error": "Internal Server Error",
    "message": "Failed to add metric values",
    "exception": "java.net.ConnectException",
    "path": "/api/v1/metrics/values"
}
```
Description:

- timestamp: the timestamp of response created
- status : the HTTP status code
- error : reason phrase of the HTTP status
- code: customized error code
- message : customized error message
- exception: fully qualified name of cause exception
- path: the requested api

***Note that:*** 'exception' field may not exist if it is caused by client error, and 'code' field may not exist for server error.

<h2 id = "1"></h2>

## Apache Griffin Basic

### Get Apache Griffin version
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

#### Request Body example 

There are two kind of different measures, Apache Griffin measure and external measure. And for each type of measure, the 'dq.type' can be 'accuracy' or 'profiling'.

Here is a request body example to create a Apache Griffin measure of  profiling:
```
{
    "name":"profiling_measure",
    "measure.type":"griffin",
    "dq.type":"profiling",
    "rule.description":{
        "details":[
            {
                "name":"age",
                "infos":"Total Count,Average"
            }
        ]
    },
    "process.type":"batch",
    "owner":"test",
    "description":"measure description",
    "data.sources":[
        {
            "name":"source",
            "connectors":[
                {
                    "name":"connector_name",
                    "type":"hive",
                    "version":"1.2",
                    "data.unit":"1hour",
                    "data.time.zone":"UTC(WET,GMT)",
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
    ],
    "evaluate.rule":{
        "rules":[
            {
                "dsl.type":"griffin-dsl",
                "dq.type":"profiling",
                "rule":"count(source.`age`) AS `age-count`,avg(source.`age`) AS `age-average`",
                "name":"profiling",
                "details":{

                }
            }
        ]
    }
}
```
And for Apache Griffin measure of accuracy:
```
{
    "name":"accuracy_measure",
    "measure.type":"griffin",
    "dq.type":"accuracy",
    "process.type":"batch",
    "owner":"test",
    "description":"measure description",
    "data.sources":[
        {
            "name":"source",
            "connectors":[
                {
                    "name":"connector_name_source",
                    "type":"HIVE",
                    "version":"1.2",
                    "data.unit":"1hour",
                    "data.time.zone":"UTC(WET,GMT)",
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
                    "data.unit":"1hour",
                    "data.time.zone":"UTC(WET,GMT)",
                    "config":{
                        "database":"default",
                        "table.name":"demo_tgt",
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
    ],
    "evaluate.rule":{
        "rules":[
            {
                "dsl.type":"griffin-dsl",
                "dq.type":"accuracy",
                "name":"accuracy",
                "rule":"source.desc=target.desc"
            }
        ]
    }
}
```
Example of request body to create external measure:
```
{
    "name": "external_name",
    "measure.type": "external",
    "dq.type": "accuracy",
    "description": "measure description",
    "organization": "orgName",
    "owner": "test",
    "metricName": "metricName"
}
```
#### Response Body Sample

The response body should be the created measure if success. For example:
```
{
    "measure.type": "griffin",
    "id": 1,
    "name": "measureName",
    "description": "measure description",
    "organization": "orgName",
    "owner": "test",
    "deleted": false,
    "dq.type": "accuracy",
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
                    "predicates": [
                        {
                            "id": 1,
                            "type": "file.exist",
                            "config": {
                                "root.path": "hdfs:///griffin/demo_src",
                                "path": "/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ],
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
                    "predicates": [
                        {
                            "id": 2,
                            "type": "file.exist",
                            "config": {
                                "root.path": "hdfs:///griffin/demo_src",
                                "path": "/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ],
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
                "name": "rule_name",
                "description": "Total count",
                "dsl.type": "griffin-dsl",
                "dq.type": "accuracy",
                "details": {}
            }
        ]
    }
}
```

### Get measures
`GET /api/v1/measures`
#### Response Body Sample
```
[
    {
        "measure.type": "griffin",
        "id": 4,
        "name": "measure_no_predicate_day",
        "owner": "test",
        "description": null,
        "organization": null,
        "deleted": false,
        "dq.type": "accuracy",
        "process.type": "batch",
        "data.sources": [
            {
                "id": 6,
                "name": "source",
                "connectors": [
                    {
                        "id": 6,
                        "name": "source1517994133405",
                        "type": "HIVE",
                        "version": "1.2",
                        "predicates": [],
                        "data.unit": "1day",
                        "data.time.zone": "UTC(WET,GMT)",
                        "config": {
                            "database": "default",
                            "table.name": "demo_src",
                            "where": "dt=#YYYYMMdd# AND hour=#HH#"
                        }
                    }
                ]
            },
            {
                "id": 7,
                "name": "target",
                "connectors": [
                    {
                        "id": 7,
                        "name": "target1517994142573",
                        "type": "HIVE",
                        "version": "1.2",
                        "predicates": [],
                        "data.unit": "1day",
                        "data.time.zone": "UTC(WET,GMT)",
                        "config": {
                            "database": "default",
                            "table.name": "demo_tgt",
                            "where": "dt=#YYYYMMdd# AND hour=#HH#"
                        }
                    }
                ]
            }
        ],
        "evaluate.rule": {
            "id": 4,
            "rules": [
                {
                    "id": 4,
                    "rule": "source.age=target.age AND source.desc=target.desc",
                    "name": "accuracy",
                    "dsl.type": "griffin-dsl",
                    "dq.type": "accuracy"
                }
            ]
        }
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

#### Request Body example 
There are two kind of different measures, Apache Griffin measure and external measure. And for each type of measure, the 'dq.type' can be 'accuracy' or 'profiling'.

Here is a request body example to update a Apache Griffin measure of accuracy:
```
{
    "id": 1,
    "name": "measureName_edit",
    "description": "measure description",
    "organization": "orgName",
    "owner": "test",
    "deleted": false,
    "dq.type": "accuracy",
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
                    "predicates": [
                        {
                            "id": 1,
                            "type": "file.exist",
                            "config": {
                                "root.path": "hdfs:///griffin/demo_src",
                                "path": "/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ],
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
                    "predicates": [
                        {
                            "id": 2,
                            "type": "file.exist",
                            "config": {
                                "root.path": "hdfs:///griffin/demo_src",
                                "path": "/dt=#YYYYMMdd#/hour=#HH#/_DONE"
                            }
                        }
                    ],
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
                "name": "rule_name",
                "description": "Total count",
                "dsl.type": "griffin-dsl",
                "dq.type": "accuracy",
                "details": {}
            }
        ]
    },
    "measure.type": "griffin"
}
```
If you want to update an external measure, you can use following example json in request body.
```
{
	"id":1,
    "measure.type": "external",
    "dq.type": "accuracy",
    "name": "external_name",
    "description": " update test measure",
    "organization": "orgName",
    "owner": "test",
    "metricName": "metricName"
}
```
#### Response Body Sample
The response body should be empty if no error happens, and the HTTP status is (204, "No Content").

### Delete measure
`DELETE /api/v1/measures/{id}`
When deleting a measure,api will also delete related jobs.
#### Path Variable
- id -`required` `Long` measure id

#### Request Sample

`/api/v1/measures/1`

#### Response Body Sample

The response body should be empty if no error happens, and the HTTP status is (204, "No Content").

### Get measure by id
`GET /api/v1/measures/{id}`
#### Path Variable
- id -`required` `Long` measure id

#### Request Sample

`/api/v1/measures/1`

#### Response Body Sample
```
{
    "measure.type": "griffin",
    "id": 4,
    "name": "measure_no_predicate_day",
    "owner": "test",
    "description": null,
    "organization": null,
    "deleted": false,
    "dq.type": "accuracy",
    "process.type": "batch",
    "data.sources": [
        {
            "id": 6,
            "name": "source",
            "connectors": [
                {
                    "id": 6,
                    "name": "source1517994133405",
                    "type": "HIVE",
                    "version": "1.2",
                    "predicates": [],
                    "data.unit": "1day",
                    "data.time.zone": "UTC(WET,GMT)",
                    "config": {
                        "database": "default",
                        "table.name": "demo_src",
                        "where": "dt=#YYYYMMdd# AND hour=#HH#"
                    }
                }
            ]
        },
        {
            "id": 7,
            "name": "target",
            "connectors": [
                {
                    "id": 7,
                    "name": "target1517994142573",
                    "type": "HIVE",
                    "version": "1.2",
                    "predicates": [],
                    "data.unit": "1day",
                    "data.time.zone": "UTC(WET,GMT)",
                    "config": {
                        "database": "default",
                        "table.name": "demo_tgt",
                        "where": "dt=#YYYYMMdd# AND hour=#HH#"
                    }
                }
            ]
        }
    ],
    "evaluate.rule": {
        "id": 4,
        "rules": [
            {
                "id": 4,
                "rule": "source.age=target.age AND source.desc=target.desc",
                "name": "accuracy",
                "dsl.type": "griffin-dsl",
                "dq.type": "accuracy"
            }
        ]
    }
}
```

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
    "measure.id": 5,
	"job.name":"job_name",
    "cron.expression": "0 0/4 * * * ?",
    "cron.time.zone": "GMT+8:00",
    "predicate.config": {
		"checkdonefile.schedule":{
			"interval": "1m",
			"repeat": 2
		}
    },
    "data.segments": [
        {
            "data.connector.name": "connector_name_source",
			"as.baseline":true, 
            "segment.range": {
                "begin": "-1h",
                "length": "1h"
            }
        },
        {
            "data.connector.name": "connector_name_target",
            "segment.range": {
                "begin": "-1h",
                "length": "1h"
            }
        }
    ]
}
```
#### Response Body Sample
The response body should be the created job schedule if success. For example:
```
{
    "id": 3,
    "measure.id": 5,
    "job.name": "job_name",
    "cron.expression": "0 0/4 * * * ?",
    "cron.time.zone": "GMT+8:00",
    "predicate.config": {
        "checkdonefile.schedule": {
            "interval": "1m",
            "repeat": 2
        }
    },
    "data.segments": [
        {
            "id": 5,
            "data.connector.name": "connector_name_source",
            "as.baseline": true,
            "segment.range": {
                "id": 5,
                "begin": "-1h",
                "length": "1h"
            }
        },
        {
            "id": 6,
            "data.connector.name": "connector_name_target",
            "as.baseline": false,
            "segment.range": {
                "id": 6,
                "begin": "-1h",
                "length": "1h"
            }
        }
    ]
}
```

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
`DELETE /api/v1/jobs/{id}`
#### Path Variable
- id -`required` `Long` job id

#### Response Body Sample

The response body should be empty if no error happens, and the HTTP status is (204, "No Content").

### Get job schedule by job name
`GET /api/v1/jobs/config/{jobName}`

#### Path Variable
- jobName -`required` `String` job name

#### Request Sample

`/api/v1/jobs/config/job_no_predicate_day`

#### Response Sample
```
{
    "id": 2,
    "measure.id": 4,
    "job.name": "job_no_predicate_day",
    "cron.expression": "0 0/4 * * * ?",
    "cron.time.zone": "GMT-8:00",
    "predicate.config": {
        "checkdonefile.schedule": {
            "repeat": "12",
            "interval": "5m"
        }
    },
    "data.segments": [
        {
            "id": 3,
            "data.connector.name": "source1517994133405",
            "as.baseline": true,
            "segment.range": {
                "id": 3,
                "begin": "-2",
                "length": "2"
            }
        },
        {
            "id": 4,
            "data.connector.name": "target1517994142573",
            "as.baseline": false,
            "segment.range": {
                "id": 4,
                "begin": "-5",
                "length": "2"
            }
        }
    ]
}
```

### Delete job by name
`DELETE /api/v1/jobs`

#### Request Parameter

| name    | description | type   | example value |
| ------- | ----------- | ------ | ------------- |
| jobName | job name    | String | job_name      |

#### Response Body Sample

The response body should be empty if no error happens, and the HTTP status is (204, "No Content").


### Get job instances
`GET /api/v1/jobs/instances`

#### Request Parameter

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

### Download sample missing/mismatched records
`GET /api/v1/jobs/download?hdfsPath={missingDataFilePath}`

#### Response
```
If successful, this method returns missing records in the response body, 
maximum record count is 100.

```

<h2 id = "4"></h2>

## Metrics

### Get metrics

`GET /api/v1/metrics`

#### Response Example
The response is a map of metrics group by measure name. For example:
```
{
    "measure_no_predicate_day": [
        {
            "name": "job_no_predicate_day",
            "type": "accuracy",
            "owner": "test",
            "metricValues": [
                {
                    "name": "job_no_predicate_day",
                    "tmst": 1517994480000,
                    "value": {
                        "total": 125000,
                        "miss": 0,
                        "matched": 125000
                    }
                },
                {
                    "name": "job_no_predicate_day",
                    "tmst": 1517994240000,
                    "value": {
                        "total": 125000,
                        "miss": 0,
                        "matched": 125000
                    }
                }
            ]
        }
    ],
    "measre_predicate_hour": [
        {
            "name": "job_predicate_hour",
            "type": "accuracy",
            "owner": "test",
            "metricValues": []
        }
    ]
}
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
The response body should have 'errors' field as 'false' if success, for example

```
{
    "took": 32,
    "errors": false,
    "items": [
        {
            "index": {
                "_index": "griffin",
                "_type": "accuracy",
                "_id": "AWFAs5pOJwYEbKWP7mhq",
                "_version": 1,
                "result": "created",
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "created": true,
                "status": 201
            }
        }
    ]
}
```

### Get metric values by name 
`GET /api/v1/metrics/values`

#### Request Parameter
name | description | type | example value
--- | --- | --- | ---
metricName | name of the metric values | String | job_no_predicate_day
size | max amount of return records | int | 5
offset | the amount of records to skip by timestamp in descending order | int | 0
tmst | the start timestamp of records you want to get | long | 0

Parameter offset and tmst are optional.
#### Response Body Sample
```
[
    {
        "name": "job_no_predicate_day",
        "tmst": 1517994720000,
        "value": {
            "total": 125000,
            "miss": 0,
            "matched": 125000
        }
    },
    {
        "name": "job_no_predicate_day",
        "tmst": 1517994480000,
        "value": {
            "total": 125000,
            "miss": 0,
            "matched": 125000
        }
    },
    {
        "name": "job_no_predicate_day",
        "tmst": 1517994240000,
        "value": {
            "total": 125000,
            "miss": 0,
            "matched": 125000
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
The response body should have 'failures' field as empty if success, for example
```
{
    "took": 363,
    "timed_out": false,
    "total": 5,
    "deleted": 5,
    "batches": 1,
    "version_conflicts": 0,
    "noops": 0,
    "retries": {
        "bulk": 0,
        "search": 0
    },
    "throttled_millis": 0,
    "requests_per_second": -1,
    "throttled_until_millis": 0,
    "failures": []
}
```

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
