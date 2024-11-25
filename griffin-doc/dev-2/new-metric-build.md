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

## Abstract
Apache Griffin 2.0 is a new generation of Data Service Platform.Compared to Griffin 1.0, new version aims to provide a
more decouple service framework, including griffin-connectors, griffin-metric, griffin-dqc, griffin-scheduler. 

Here, we will give much more details about griffin-metric.

## Apache Griffin Metric
You can use dev profile to verify the metric API.  
1. Edit the configuration file griffin-metric/src/main/resources/application.yaml
```yaml
spring:
  profiles:
    active: dev # set effective configuration to application-dev.yaml
```
2. Run the metric service
```shell
$JAVA_HOME/bin/java -cp <classpath> org.apache.griffin.metric.DAOApplication
```
3. Check the H2 memory database
```shell
Explore http://localhost:8888/h2-console by the broswer

Fill 'JDBC URL' with 'jdbc:h2:mem:griffin'

Click the button 'Connect'

You can see four tables:
T_METRIC_D 
T_METRIC_V
T_METRIC_TAG  
T_TAG_D 
```
4. Call REST APIs
   1. Query all metric definitions
    ```shell
    GET http://localhost:8888/allMetricDs
   
    ```
    2. 

http://127.0.0.1:8888/h2-console/login.do?jsessionid=787b7279ce05baa951acf0abfdd60df5

###
GET http://localhost:8888/ping

###
GET http://localhost:8888/allMetricDs

<> 2024-10-09T211552.500.json
<> 2024-10-09T211458.200.json
<> 2024-10-09T211446.200.json
<> 2024-10-09T204950.200.json
<> 2024-10-09T195018.200.json
<> 2024-10-08T223000.200.json
<> 2024-10-08T222937.200.json
<> 2024-10-08T222713.200.json
<> 2024-10-08T222237.200.json

###
PUT http://localhost:8888/metricD
Content-Type: application/json

{
"description": "test metric",
"metricName": "latency",
"owner": "admin"
}

<> 2024-10-14T221213.201.json
<> 2024-10-14T220832.201.json
<> 2024-10-14T220539.201.json
<> 2024-10-14T220401.201.json
<> 2024-10-14T220108.201.json
<> 2024-10-14T220024.201.json
<> 2024-10-14T215934.201.json
<> 2024-10-14T215227.201.json
<> 2024-10-14T213212.201.json
<> 2024-10-10T222838.201.json
<> 2024-10-09T225958.400.json
<> 2024-10-09T225404.200.json
<> 2024-10-09T211450.200.json
<> 2024-10-09T204917.200.json
<> 2024-10-08T222650.200.json
<> 2024-10-08T222209.200.json
<> 2024-10-08T213859.200.json
<> 2024-10-08T212816.200.json
<> 2024-10-08T212648.200.json

###
PUT http://localhost:8888/metricV
Content-Type: application/json

{
"metricId": 1,
"value": 5,
"ctime": "",
"mtime": ""
}

<> 2024-10-09T211455.200.json
<> 2024-10-09T204935.200.json
<> 2024-10-08T212659.200.json
<> 2024-10-07T215443.200.json

###
PUT http://localhost:8888/metricTagD
Content-Type: application/json

{
"tagKey": "perf",
"tagValue": "0.0"
}

<> 2024-10-08T222656.200.json
<> 2024-10-08T222214.200.json
<> 2024-10-08T213904.200.json
<> 2024-10-08T212826.200.json
<> 2024-10-08T212703.500.json
<> 2024-10-07T230617.200.json

###
PUT http://localhost:8888/tags
Content-Type: application/json

{
"metricId": 1,
"tagId": 1
}



<> 2024-10-08T222659.200.json
<> 2024-10-08T222218.200.json
<> 2024-10-08T214154.200.json
<> 2024-10-08T213912.200.json
<> 2024-10-08T212923.500.json
<> 2024-10-07T230658.200.json
<> 2024-10-07T214653.500.json

###
DELETE http://localhost:8888/metricD/1
