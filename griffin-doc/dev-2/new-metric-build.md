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
      
         Response:
         [
            {
               "metricId": 1,
               "metricName": "latency",
               "owner": "admin",
               "description": "test metric",
               "creation_time": "2024-11-29T03:17:06.671+00:00",
               "update_time": "2024-11-29T03:17:06.671+00:00"
            }
         ]
       ```
   2. Define a new metric
      ```shell
        PUT http://localhost:8888/metricD
        Content-Type: application/json
        {
           "description": "test metric",
           "metricName": "latency",
           "owner": "admin"
        }
       
        Response:
        {
           "metricId": 1,
           "metricName": "latency",
           "owner": "admin",
           "description": "test metric"
        }
        ```
   3. Set the value of a metric 
        ```shell
        PUT http://localhost:8888/metricV
        Content-Type: application/json
      
        {
           "metricId": 1,
           "value": 5,
           "ctime": "",
           "mtime": ""
        }
      
        Response:
        {
           "metricId": 1,
           "value": 5.0
        }
        ```
   4. Define a metric tag
        ```shell
        PUT http://localhost:8888/metricTagD
      Content-Type: application/json
      
      {
          "tagKey": "perf",
          "tagValue": "0.0"
      }
   
        Response:
        {
            "id": 1,
            "tagKey": "perf",
            "tagValue": "0.0"
      }
        ```
   5. Tag a metric definition
      ```shell
       PUT http://localhost:8888/tags
       Content-Type: application/json

      {
      "metricId": 1,
      "tagId": 1
      }
      
      Response:
      {
      "metricId": 1,
      "tagId": 1
      }
      ```
   5. Delete a metric definition
      ```shell
      DELETE http://localhost:8888/metricD/{id}
      ```

