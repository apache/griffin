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

# Apache Griffin Development Environment Build Guide
We have pre-built Griffin docker images for Griffin developers. You can use the images directly, which should be much faster than building the environment locally.

## Set Up with Docker Images
Here are step-by-step instructions on how to [pull Docker images](../docker/griffin-docker-guide.md#environment-preparation) from the repository and run containers using the images.

## Run or Debug at local
### For service module
If you need to develop the service module, you need to modify some configuration in the following files.
Docker host is your machine running the docker containers, which means if you install docker and run docker containers on 192.168.100.100, then the `<docker host ip>` is 192.168.100.100.

In service/src/main/resources/application.properties
```
spring.datasource.url = jdbc:postgresql://<docker host ip>:35432/quartz?autoReconnect=true&useSSL=false

hive.metastore.uris = thrift://<docker host ip>:39083

elasticsearch.host = <docker host ip>
elasticsearch.port = 39200
```

In service/src/main/resources/sparkJob.properties
```
livy.uri=http://<docker host ip>:38998/batches

spark.uri=http://<docker host ip>:38088
```

Now you can start the service module in your local IDE, by running or debugging org.apache.griffin.core.GriffinWebApplication.

### For ui module
If you need to develop the ui module only, you need to modify some configuration.

In ui/angular/src/app/service/service.service.ts
```
// public BACKEND_SERVER = "";
public BACKEND_SERVER = 'http://<docker host ip>:38080';
```
After this, you can test your ui module by using remote service.

However, in most conditions, you need to develop the ui module with some modification in service module.
Then you need to follow the steps above for service module first, and
In ui/angular/src/app/service/service.service.ts
```
// public BACKEND_SERVER = "";
public BACKEND_SERVER = 'http://localhost:8080';
```
After this, you can start service module at local, and test your ui module by using local service.

### For measure module
If you need to develop the measure module only, you can ignore any of the service or ui module.
You can test your built measure JAR in the docker container, using the existed spark environment.

For debug phase, you'd better install hadoop, spark, hive at local, and test your program at local for fast.

## Deploy on docker container
First, in the incubator-griffin directory, build you packages at once.
```
mvn clean install
```

### For service module and ui module
1. Login to docker container, and stop running griffin service.
```
docker exec -it <griffin docker container id> bash
cd ~/service
ps -ef | grep service.jar
kill -9 <pid of service.jar>
```
2. Service and ui module are both packaged in `service/target/service-<version>.jar`, copy it into your docker container.
```
docker cp service-<version>.jar <griffin docker container id>:/root/service/service.jar
```
3. In docker container, start the new service.
```
cd ~/service
nohup java -jar service.jar > service.log &
```
Now you can follow the service log by `tail -f service.log`.

### For measure module
1. Measure module is packaged in `measure/target/measure-<version>.jar`, copy it into your docker container.
```
docker cp measure-<version>.jar <griffin docker container id>:/root/measure/griffin-measure.jar
```
2. Login to docker container, and overwrite griffin-measure.jar onto hdfs inside.
```
docker exec -it <griffin docker container id> bash
hadoop fs -rm /griffin/griffin-measure.jar
hadoop fs -put /root/measure/griffin-measure.jar /griffin/griffin-measure.jar
```
Now the griffin service will submit jobs by using this new griffin-measure.jar.

## Build new griffin docker image
For end2end test, you will need to build a new griffin docker image, for more convenient test.
1. Pull the docker build repo on your docker host.
```
git clone https://github.com/bhlx3lyx7/griffin-docker.git
```
2. Copy your measure and service JAR into griffin_spark2 directory.
```
cp service-<version>.jar <path to>/griffin-docker/griffin_spark2/prep/service/service.jar
cp measure-<version>.jar <path to>/griffin-docker/griffin_spark2/prep/measure/griffin-measure.jar
```
3. Build your new griffin docker image.
In griffin_spark2 directory.
```
cd <path to>/griffin-docker/griffin_spark2
docker build -t <image name>[:<image version>] .
```
4. If you are using another image name (or version), you need also modify the docker-compose file you're using.
```
griffin:
  image: <image name>[:<image version>]
```
5. Now you can run your new griffin docker image.
```
docker-compose -f <docker-compose file> up -d
```
