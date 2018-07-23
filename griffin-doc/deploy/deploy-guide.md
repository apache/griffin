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

# Apache Griffin Deployment Guide
For Griffin users, please follow the instructions below to deploy Griffin in your environment. Note that there are some dependencies should be installed first.

### Prerequisites
You need to install following items
- jdk (1.8 or later versions).
- Postgresql or Mysql.
- npm (version 6.0.0+).
- [Hadoop](http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz) (2.6.0 or later), you can get some help [here](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html).
-  [Spark](http://spark.apache.org/downloads.html) (version 2.2.1), if you want to install Pseudo Distributed/Single Node Cluster, you can get some help [here](http://why-not-learn-something.blogspot.com/2015/06/spark-installation-pseudo.html).
- [Hive](http://apache.claz.org/hive/hive-2.2.0/apache-hive-2.2.0-bin.tar.gz) (version 2.2.0), you can get some help [here](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-RunningHive).
    You need to make sure that your spark cluster could access your HiveContext.
- [Livy](http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip), you can get some help [here](http://livy.io/quickstart.html).
    Griffin need to schedule spark jobs by server, we use livy to submit our jobs.
    For some issues of Livy for HiveContext, we need to download 3 files or get them from Spark lib `$SPARK_HOME/lib/`, and put them into HDFS.
    ```
    datanucleus-api-jdo-3.2.6.jar
    datanucleus-core-3.2.10.jar
    datanucleus-rdbms-3.2.9.jar
    ```
- ElasticSearch (5.0 or later).
	ElasticSearch works as a metrics collector, Griffin produces metrics to it, and our default UI get metrics from it, you can use your own way as well.

### Configuration

#### Postgresql

Create database 'quartz' in postgresql
```
createdb -O <username> quartz
```
Init quartz tables in postgresql by [Init_quartz_postgres.sql](../../service/src/main/resources/Init_quartz_postgres.sql)
```
psql -p <password> -h <host address> -U <username> -f Init_quartz_postgres.sql quartz
```

#### Mysql

Create database 'quartz' in mysql
```
mysql -u <username> -e "create database quartz" -p
```
Init quartz tables in mysql by [Init_quartz_mysql_innodb.sql.sql](../../service/src/main/resources/Init_quartz_mysql_innodb.sql)
```
mysql -u <username> -p quartz < Init_quartz_mysql_innodb.sql.sql
```


You should also modify some configurations of Griffin for your environment.

- <b>service/src/main/resources/application.properties</b>

    ```
    # jpa
    spring.datasource.url = jdbc:postgresql://<your IP>:5432/quartz?autoReconnect=true&useSSL=false
    spring.datasource.username = <user name>
    spring.datasource.password = <password>
    spring.jpa.generate-ddl=true
    spring.datasource.driverClassName = org.postgresql.Driver
    spring.jpa.show-sql = true

    # hive metastore
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

- <b>measure/src/main/resources/env-streaming.json</b>
	```
	"persist": [
	    ...
	    {
			"type": "http",
			"config": {
		        "method": "post",
		        "api": "http://<your ES IP>:<ES rest port>/griffin/accuracy"
			}
		}
	]
	```
	Rename the modified env-streaming.json file as env.json and put it into HDFS.

- <b>service/src/main/resources/sparkJob.properties</b>
    ```
    sparkJob.file = hdfs://<griffin measure path>/griffin-measure.jar
    sparkJob.args_1 = hdfs://<griffin env path>/env.json

    # other dependent jars
    sparkJob.jars =

    # hive-site.xml location
    spark.yarn.dist.files = hdfs://<path to>/hive-site.xml

    livy.uri = http://<your IP>:8998/batches
    yarn.uri = http://<your IP>:8088
    ```
    - \<griffin measure path> is the location you should put the jar file of measure module.
    - \<griffin env path> is the location you should put the env.json file.

### Build and Run

Build the whole project and deploy. (NPM should be installed)

  ```
  mvn clean install
  ```

Put jar file of measure module into \<griffin measure path> in HDFS

```
cp measure/target/measure-<version>-incubating-SNAPSHOT.jar measure/target/griffin-measure.jar
hdfs dfs -put measure/target/griffin-measure.jar <griffin measure path>/
  ```

After all environment services startup, we can start our server.

  ```
  java -jar service/target/service.jar
  ```

After a few seconds, we can visit our default UI of Griffin (by default the port of spring boot is 8080).

  ```
  http://<your IP>:8080
  ```

You can use UI following the steps [here](../ui/user-guide.md).

**Note**: The UI does not support all the backend features, to experience the advanced features you can use services directly.

