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

# Griffin Measure Demo Docker
We've prepared a docker for griffin measure demo.

## Preparation
1. Install [docker](https://docs.docker.com/engine/installation/).
2. Download docker image. In this image, the environment for measure module has been prepared, including: hadoop, hive, spark, mysql.  
```
docker pull bhlx3lyx7/griffin_measure_demo:0.0.1
```
3. Run docker image.  
```
docker run -it -h griffin --name griffin_measure_demo -m 8G --memory-swap -1 \
-p 42122:2122 -p 47077:7077 -p 46066:6066 -p 48088:8088 -p 48040:8040 \
-p 43306:3306 -p 49000:9000 -p 48042:8042 -p 48080:8080 -p 47017:27017 \
-p 49083:9083 -p 48998:8998 -p 49200:9200 bhlx3lyx7/griffin_measure_demo:0.0.1
```
4. In this docker container, run the prepared demo.
- **accuracy demo**: This demo is batch accuracy, source data is Hive table "demo_src", target data is Hive table "demo_tgt", metrics will be persisted in `hdfs:///griffin/persist/accu` after calculation.
	+ switch into `job/accu`.
		```
		cd job/accu
		```
	+ run the prepared script.
		```
		./bgwork.sh
		```
	+ check job log.
		```
		tail -f accu.log
		```
- **profiling demo**: This demo is batch profiling, source data is Hive table "demo_src", metrics will be persisted in `hdfs:///griffin/persist/prof` after calculation.
	+ switch into `job/prof`.
		```
		cd job/prof
		```
	+ run the prepared script.
		```
		./bgwork.sh
		```
	+ check job log.
		```
		tail -f prof.log
		```
5. You can modify the job configuration file `config.json` of the above demos, or create your own data sources, to get more metrics of data.