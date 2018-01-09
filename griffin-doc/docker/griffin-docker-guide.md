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

# Apache Griffin Docker Guide
Griffin docker images are pre-built on docker hub, users can pull them to try griffin in docker.

## Preparation

### Environment preparation
1. Install [docker](https://docs.docker.com/engine/installation/) and [docker compose](https://docs.docker.com/compose/install/).
2. Increase vm.max_map_count of your local machine, to use elasticsearch.
    ```
    sysctl -w vm.max_map_count=262144
    ```
3. Pull griffin pre-built docker images.
    ```
    docker pull bhlx3lyx7/svc_msr:0.1.6
    docker pull bhlx3lyx7/elasticsearch
    docker pull bhlx3lyx7/kafka
    docker pull zookeeper:3.5
    ```
   Or you can pull the images faster through mirror acceleration if you are in China.
    ```
    docker pull registry.docker-cn.com/bhlx3lyx7/svc_msr:0.1.6
    docker pull registry.docker-cn.com/bhlx3lyx7/elasticsearch
    docker pull registry.docker-cn.com/bhlx3lyx7/kafka
    docker pull registry.docker-cn.com/zookeeper:3.5
    ```
   The docker images are the griffin environment images.
    - `bhlx3lyx7/svc_msr`: This image contains mysql, hadoop, hive, spark, livy, griffin service, griffin measure, and some prepared demo data, it works as a single node spark cluster, providing spark engine and griffin service.
    - `bhlx3lyx7/elasticsearch`: This image is based on official elasticsearch, adding some configurations to enable cors requests, to provide elasticsearch service for metrics persist.
    - `bhlx3lyx7/kafka`: This image contains kafka 0.8, and some demo streaming data, to provide streaming data source in streaming mode.
    - `zookeeper:3.5`: This image is official zookeeper, to provide zookeeper service in streaming mode.

### How to use griffin docker images in batch mode
1. Copy [docker-compose-batch.yml](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/docker/svc_msr/docker-compose-batch.yml) to your work path.
2. In your work path, start docker containers by using docker compose, wait for about one minutes, then griffin service is ready.
    ```
    docker-compose -f docker-compose-batch.yml up -d
    ```
3. Now you can try griffin APIs by using postman after importing the [json files](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/postman).
    In which you need to modify the environment `BASE_PATH` value into `<your local IP address>:38080`.
4. You can try the api `Basic -> Get griffin version`, to make sure griffin service has started up.
5. Add an accuracy measure through api `Measures -> Add measure`, to create a measure in griffin.
6. Add a job to through api `jobs -> Add job`, to schedule a job to execute the measure. In the example, the schedule interval is 5 minutes.
7. After some minutes, you can get the metrics from elasticsearch.
    ```
    curl -XGET '<your local IP address>:39200/griffin/accuracy/_search?pretty&filter_path=hits.hits._source' -d '{"query":{"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}]}'
    ```

### How to use griffin docker images in streaming mode
1. Copy [docker-compose-streaming.yml](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/docker/svc_msr/docker-compose-streaming.yml) to your work path.
2. In your work path, start docker containers by using docker compose, wait for about one minutes, then griffin service is ready.
    ```
    docker-compose -f docker-compose-streaming.yml up -d
    ```
3. Enter the griffin docker container.
    ```
    docker exec -it griffin bash
    ```
4. Switch into the measure directory.
    ```
    cd ~/measure
    ```
5. Execute the script of streaming-accu, to execute streaming accuracy measurement.
    ```
    ./streaming-accu.sh
    ```
   You can trace the log in streaming-accu.log.
    ```
    tail -f streaming-accu.log
    ```
6. Limited by the docker container resource, you can only execute accuracy or profiling separately.
   If you want to try streaming profiling measurement, please kill the streaming-accu process first.
    ```
    kill -9 `ps -ef | awk '/griffin-measure/{print $2}'`
    ```
   Then clear the checkpoint directory and other related directories of last streaming job.
    ```
    ./clear.sh
    ```
   Execute the script of streaming-prof, to execute streaming profiling measurement.
    ```
    ./streaming-prof.sh
    ```
   You can trace the log in streaming-prof.log.
    ```
    tail -f streaming-prof.log
    ```