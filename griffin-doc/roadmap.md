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
# Apache Griffin Roadmap

## Current feature list
In the current version, we've implemented the below main DQ features

- **Data Asset Detection**
  After configuration in service module, Griffin can detect the Hive tables metadata through Hive metastore service.

- **Measure Management**
  Through UI, user can create, delete measures for 3 types: accuracy, profiling and publish metrics.
  Through service API, user can create, delete and update measures for 6 types: accuracy, profiling, timeliness, uniqueness, completeness and publish metrics.

- **Job Management**
  User can create, delete job to schedule batch job for calculative measures, data range of each calculation, and the extra trigger condition like "done file" on hdfs.

- **Measure Calculation on Spark**
  Service module will trigger and submit calculation jobs to Spark cluster through livy, the measure module calculates and persists the metric values to elasticsearch by default.

- **Metrics Visualization**
  Through service API, user can get metric values of each job from elasticsearch.
  On UI, accuracy metrics will be rendered as a chart, profiling metrics will be displayed as a table.


## Short-term Roadmap

- **Support more data source types**
  At current, Griffin only supports Hive table, avro files on hdfs as data source in batch mode, Kafka as data source in streaming mode.
  We plan to support more data source types, like RDBM, elasticsearch.

- **Support more data quality dimensions**
  Griffin need to support more data quality dimensions, like consistency and validity.

- **Anomaly Detection**
  Griffin plan to support anomaly detection, by analyzing calculated metrics from elasticsearch.