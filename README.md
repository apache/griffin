## Apache Griffin

Apache Griffin is a model driven data quality solution for modern data systems. 
It provides a standard process to define data quality measures, execute, report, as well as an unified dashboard across multiple data systems. 
You can access our home page [here](http://griffin.incubator.apache.org/).
You can access our wiki page [here](https://cwiki.apache.org/confluence/display/GRIFFIN/Apache+Griffin).
You can access our issues jira page [here](https://issues.apache.org/jira/secure/Dashboard.jspa?selectPageId=12330914).

### Contact us
[Dev List](mailto://dev@griffin.incubator.apache.org)
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

### CI


### Repository
Snapshot:

Release:

### How to run in docker
1. Install [docker](https://www.docker.com/).
2. Pull our built docker image.
    ```
    docker pull bhlx3lyx7/griffin_demo:0.0.1
    ```
3. Increase vm.max_map_count of your local machine, to use elasticsearch.  
    ```
    sysctl -w vm.max_map_count=262144
    ```  
4. Run this docker image, wait for about one minute, then griffin is ready.
    ```
    docker run -it -h sandbox --name griffin_demo -m 8G --memory-swap -1 \
    -p 32122:2122 -p 37077:7077 -p 36066:6066 -p 38088:8088 -p 38040:8040 \
    -p 33306:3306 -p 39000:9000 -p 38042:8042 -p 38080:8080 -p 37017:27017 \
    -p 39083:9083 -p 38998:8998 -p 39200:9200 bhlx3lyx7/griffin_demo:0.0.1
    ```
5. Now you can visit UI through your browser, login with account "test" and password "test" if required.
    ```
    http://<your local IP address>:38080/
    ```
    You can also follow the steps using UI [here](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/dockerUIguide.md#webui-test-case-guide).

### How to deploy and run at local
1. Install jdk (1.8 or later versions).
2. Install mysql.
2. Install [Hadoop](http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz) (2.6.0 or later), you can get some help [here](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html).
3. Install [Spark](http://spark.apache.org/downloads.html) (version 1.6.x, griffin does not support 2.0.x at current), if you want to install Pseudo Distributed/Single Node Cluster, you can get some help [here](http://why-not-learn-something.blogspot.com/2015/06/spark-installation-pseudo.html).
4. Install [Hive](http://apache.claz.org/hive/hive-1.2.1/apache-hive-1.2.1-bin.tar.gz) (version 1.2.1 or later), you can get some help [here](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-RunningHive).
    You need to make sure that your spark cluster could access your HiveContext.
5. Install [Livy](http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip), you can get some help [here](http://livy.io/quickstart.html).
    Griffin need to schedule spark jobs by server, we use livy to submit our jobs.
    For some issues of Livy for HiveContext, we need to download 3 files, and put them into Hdfs.
    ```
    datanucleus-api-jdo-3.2.6.jar
    datanucleus-core-3.2.10.jar
    datanucleus-rdbms-3.2.9.jar
    ```
6. Install [ElasticSearch]().
    ElasticSearch works as a metrics collector, Griffin produces metrics to it, and our default UI get metrics from it, you can use your own way as well.
7. Modify configuration for your environment.
    You need to modify the configuration part of code, to make Griffin works well in you environment.
    service/src/main/resources/application.properties
    ```
    spring.datasource.url = jdbc:mysql://<your IP>:3306/quartz?autoReconnect=true&useSSL=false
    spring.datasource.username = <user name>
    spring.datasource.password = <password>

    hive.metastore.uris = thrift://<your IP>:9083
    hive.metastore.dbname = <hive database name>    # default is "default"
    ```
    service/src/main/resources/sparkJob.properties
    ```
    sparkJob.file = hdfs://<griffin measure path>/griffin-measure.jar
    sparkJob.args_1 = hdfs://<griffin env path>/env.json
    sparkJob.jars_1 = hdfs://<datanucleus path>/datanucleus-api-jdo-3.2.6.jar
    sparkJob.jars_2 = hdfs://<datanucleus path>/datanucleus-core-3.2.10.jar
    sparkJob.jars_3 = hdfs://<datanucleus path>/datanucleus-rdbms-3.2.9.jar
    sparkJob.uri = http://<your IP>:8998/batches
    ```
    ui/js/services/services.js
    ```
    ES_SERVER = "http://<your IP>:9200"
    ```
    Configure measure/measure-batch/src/main/resources/env.json for your environment, and put it into Hdfs <griffin env path>/
8. Build the whole project and deploy.
    ```
    mvn install
    ```
    Create a directory in Hdfs, and put our measure package into it.
    ```
    cp /measure/target/measure-0.1.3-incubating-SNAPSHOT.jar /measure/target/griffin-measure.jar
    hdfs dfs -put /measure/target/griffin-measure.jar <griffin measure path>/
    ```
    After all our environment services startup, we can start our server.
    ```
    java -jar service/target/service.jar
    ```
    After a few seconds, we can visit our default UI of Griffin (by default the port of spring boot is 8080).
    ```
    http://<your IP>:8080
    ```
9. Follow the steps using UI [here](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/dockerUIguide.md#webui-test-case-guide).


**Note**: The front-end UI is still under development, you can only access some basic features currently.


### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute code, documentation, etc.
