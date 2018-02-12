

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


# Apache Griffin  
[![Build Status](https://travis-ci.org/apache/incubator-griffin.svg?branch=master)](https://travis-ci.org/apache/incubator-griffin) [![License: Apache 2.0](https://camo.githubusercontent.com/8cb994f6c4a156c623fe057fccd7fb7d7d2e8c9b/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f6c6963656e73652d417061636865253230322d3445423142412e737667)](https://www.apache.org/licenses/LICENSE-2.0.html)    

Apache Griffin is a model driven data quality solution for modern data systems. It provides a standard process to define data quality measures, execute, report, as well as an unified dashboard across multiple data systems. 

## Getting Started


You can try Griffin in docker following the [docker guide](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/docker/griffin-docker-guide.md).

To run Griffin at local, you can follow instructions below.

### Prerequisites
You need to install following items 
- jdk (1.8 or later versions).
- mysql.
- npm (version 6.0.0+).
- [Hadoop](http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz) (2.6.0 or later), you can get some help [here](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html).
-  [Spark](http://spark.apache.org/downloads.html) (version 1.6.x, griffin does not support 2.0.x at current), if you want to install Pseudo Distributed/Single Node Cluster, you can get some help [here](http://why-not-learn-something.blogspot.com/2015/06/spark-installation-pseudo.html).
- [Hive](http://apache.claz.org/hive/hive-1.2.1/apache-hive-1.2.1-bin.tar.gz) (version 1.2.1 or later), you can get some help [here](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-RunningHive).
    You need to make sure that your spark cluster could access your HiveContext.
- [Livy](http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip), you can get some help [here](http://livy.io/quickstart.html).
    Griffin need to schedule spark jobs by server, we use livy to submit our jobs.
    For some issues of Livy for HiveContext, we need to download 3 files, and put them into HDFS.
    ```
    datanucleus-api-jdo-3.2.6.jar
    datanucleus-core-3.2.10.jar
    datanucleus-rdbms-3.2.9.jar
    ```
- ElasticSearch. 
	ElasticSearch works as a metrics collector, Griffin produces metrics to it, and our default UI get metrics from it, you can use your own way as well.

### Configuration

Create a griffin working directory in HDFS
```
hdfs dfs -mkdir -p <griffin working dir>
```
Init quartz tables in mysql by service/src/main/resources/Init_quartz.sql
```
mysql -u username -p quartz < service/src/main/resources/Init_quartz.sql
```


You should also modify some configurations of Griffin for your environment.

- <b>service/src/main/resources/application.properties</b>

    ```
    # mysql
    spring.datasource.url = jdbc:mysql://<your IP>:3306/quartz?autoReconnect=true&useSSL=false
    spring.datasource.username = <user name>
    spring.datasource.password = <password>
    
    # hive
    hive.metastore.uris = thrift://<your IP>:9083
    hive.metastore.dbname = <hive database name>    # default is "default"
    
    # external properties directory location, ignore it if not required
    external.config.location =

	# login strategy, default is "default"
	login.strategy = <default or ldap>

	# ldap properties, ignore them if ldap is not enabled
	ldap.url = ldap://hostname:port
	ldap.email = @example.com
	ldap.searchBase = DC=org,DC=example
	ldap.searchPattern = (sAMAccountName={0})

	# hdfs, ignore it if you do not need predicate job
	fs.defaultFS = hdfs://<hdfs-default-name>

	# elasticsearch
	elasticsearch.host = <your IP>
	elasticsearch.port = <your elasticsearch rest port>
	# authentication properties, uncomment if basic authentication is enabled
	# elasticsearch.user = user
	# elasticsearch.password = password
    ```
- <b>service/src/main/resources/sparkJob.properties</b>
    ```
    sparkJob.file = hdfs://<griffin measure path>/griffin-measure.jar
    sparkJob.args_1 = hdfs://<griffin env path>/env.json
    
    sparkJob.jars = hdfs://<datanucleus path>/spark-avro_2.11-2.0.1.jar\
	    hdfs://<datanucleus path>/datanucleus-api-jdo-3.2.6.jar\
	    hdfs://<datanucleus path>/datanucleus-core-3.2.10.jar\
	    hdfs://<datanucleus path>/datanucleus-rdbms-3.2.9.jar
	    
	spark.yarn.dist.files = hdfs:///<spark conf path>/hive-site.xml
	
    livy.uri = http://<your IP>:8998/batches
    spark.uri = http://<your IP>:8088
    ```
    You should put these files into the same path as you set above in HDFS

- <b>measure/src/main/resources/env.json</b> 
	```
	"persist": [
	    ...
	    {
			"type": "http",
			"config": {
		        "method": "post",
		        "api": "http://<your ES IP>:<port>/griffin/accuracy"
			}
		}
	]
	```
	Put this env.json file of measure module into \<griffin env path> in HDFS.
	
### Build and Run

Build the whole project and deploy. (NPM should be installed)

  ```
  mvn install
  ```
 
Put jar file of measure module into \<griffin measure path> in HDFS
```
cp measure/target/measure-<version>-incubating-SNAPSHOT.jar /measure/target/griffin-measure.jar
hdfs dfs -put /measure/target/griffin-measure.jar <griffin measure path>/
  ```

After all environment services startup, we can start our server.

  ```
  java -jar service/target/service.jar
  ```
    
After a few seconds, we can visit our default UI of Griffin (by default the port of spring boot is 8080).

  ```
  http://<your IP>:8080
  ```

You can use UI following the steps  [here](https://github.com/apache/incubator-griffin/blob/master/griffin-doc/ui/user-guide.md).

**Note**: The front-end UI is still under development, you can only access some basic features currently.

## Community

You can contact us via email: <a href="mailto:dev@griffin.incubator.apache.org">dev@griffin.incubator.apache.org</a>

You can also subscribe this mail by sending a email to [here](mailto:dev-subscribe@griffin.incubator.apache.org). 

You can access our issues jira page [here](https://issues.apache.org/jira/browse/GRIFFIN)



## Contributing

See [Contributing Guide](./CONTRIBUTING.md) for details on how to contribute code, documentation, etc.

## References
- [Home Page](http://griffin.incubator.apache.org/)
- [Wiki](https://cwiki.apache.org/confluence/display/GRIFFIN/Apache+Griffin)
- Documents:
	- [Measure](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/measure)
	- [Service](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/service)
	- [UI](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/ui)
	- [Docker usage](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/docker)
	- [Postman API](https://github.com/apache/incubator-griffin/tree/master/griffin-doc/service/postman)
