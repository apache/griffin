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
For Apache Griffin users, please follow the instructions below to deploy Apache Griffin in your environment. Note that there are some dependencies that should be installed firstly.

### Prerequisites
Firstly you need to install and configure following software products, here we use [ubuntu-18.10](https://www.ubuntu.com/download) as sample OS to prepare all dependencies.
```bash
# put all download packages into /apache folder
$ mkdir /home/user/software
$ sudo ln -s /home/user/software /apache
$ sudo ln -s /apache/data /data
```

- JDK (1.8 or later versions)
```bash
$ sudo apt install openjdk-8-jre-headless

$ java -version
openjdk version "1.8.0_191"
OpenJDK Runtime Environment (build 1.8.0_191-8u191-b12-0ubuntu0.18.10.1-b12)
OpenJDK 64-Bit Server VM (build 25.191-b12, mixed mode)
```

- PostgreSQL(version 10.4) or MySQL(version 8.0.11)
```bash
# PostgreSQL
$ sudo apt install postgresql-10

# MySQL
$ sudo apt install mysql-server-5.7
```

- [npm](https://nodejs.org/en/download/) (version 6.0.0+)
```bash
$ sudo apt install nodejs
$ sudo apt install npm
$ node -v
$ npm -v
```

- [Hadoop](http://apache.claz.org/hadoop/common/) (2.6.0 or later), you can get some help [here](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html).

- [Hive](http://apache.claz.org/hive/) (version 2.x), you can get some help [here](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-RunningHive).

- [Spark](http://spark.apache.org/downloads.html) (version 2.2.1), if you want to install Pseudo Distributed/Single Node Cluster, you can get some help [here](http://why-not-learn-something.blogspot.com/2015/06/spark-installation-pseudo.html).

- [Livy](http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip), you can get some help [here](http://livy.io/quickstart.html).

- [ElasticSearch](https://www.elastic.co/downloads/elasticsearch) (5.0 or later versions).
	ElasticSearch works as a metrics collector, Apache Griffin produces metrics into it, and our default UI gets metrics from it, you can use them by your own way as well.

### Configuration

#### PostgreSQL

Create database 'quartz' in PostgreSQL
```
createdb -O <username> quartz
```
Init quartz tables in PostgreSQL using [Init_quartz_postgres.sql](../../service/src/main/resources/Init_quartz_postgres.sql)
```
psql -p <port> -h <host address> -U <username> -f Init_quartz_postgres.sql quartz
```

#### MySQL

Create database 'quartz' in MySQL
```
mysql -u <username> -e "create database quartz" -p
```
Init quartz tables in MySQL using [Init_quartz_mysql_innodb.sql](../../service/src/main/resources/Init_quartz_mysql_innodb.sql)
```
mysql -u <username> -p quartz < Init_quartz_mysql_innodb.sql
```

#### Set Env

export those variables below, or create hadoop_env.sh and put it into .bashrc
```bash
#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

export HADOOP_HOME=/apache/hadoop
export HADOOP_COMMON_HOME=/apache/hadoop
export HADOOP_COMMON_LIB_NATIVE_DIR=/apache/hadoop/lib/native
export HADOOP_HDFS_HOME=/apache/hadoop
export HADOOP_INSTALL=/apache/hadoop
export HADOOP_MAPRED_HOME=/apache/hadoop
export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/apache/spark
export LIVY_HOME=/apache/livy
export HIVE_HOME=/apache/hive
export YARN_HOME=/apache/hadoop
export SCALA_HOME=/apache/scala
```

#### Hadoop

* **update configuration**

here are sample configurations for hadoop<br>
Put site-specific property overrides in this file **/apache/hadoop/etc/hadoop/core-site.xml**
```xml
<configuration>
    <name>fs.defaultFS</name>
    <value>hdfs://127.0.0.1:9000</value>
</configuration>
```

Put site-specific property overrides in this file **/apache/hadoop/etc/hadoop/hdfs-site.xml**
```xml
<configuration>
    <property>
        <name>dfs.namenode.logging.level</name>
        <value>warn</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.servicerpc-address</name>
        <value>127.0.0.1:9001</value>
    </property>
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>127.0.0.1:9002</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///data/hadoop-data/nn</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///data/hadoop-data/dn</value>
    </property>
    <property>
        <name>dfs.namenode.checkpoint.dir</name>
        <value>file:///data/hadoop-data/snn</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
</configuration>
```

* **start/stop hadoop nodes**
```bash
# format name node
/apache/hadoop/bin/hdfs namenode -format
# start namenode/datanode
/apache/hadoop/sbin/start-dfs.sh
# stop all nodes
/apache/hadoop/sbin/stop-all.sh
```
* **start/stop hadoop ResourceManager**
```bash
# manually clear the ResourceManager state store
/apache/hadoop/bin/yarn resourcemanager -format-state-store
# startup the ResourceManager
/apache/hadoop/sbin/yarn-daemon.sh start resourcemanager
# stop the ResourceManager
/apache/hadoop/sbin/yarn-daemon.sh stop resourcemanager
```
* **start/stop hadoop NodeManager**
```bash
# startup the NodeManager
/apache/hadoop/sbin/yarn-daemon.sh start nodemanager
# stop the NodeManager
/apache/hadoop/sbin/yarn-daemon.sh stop nodemanager
```
* **start/stop hadoop HistoryServer**
```bash
# startup the HistoryServer
/apache/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
# stop the HistoryServer
/apache/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver
```

#### Hive
You need to make sure that your spark cluster could access your HiveContext.
* **update configuration**
Copy hive/conf/hive-site.xml.template to hive/conf/hive-site.xml and update some fields.
```xml
+++ hive/conf/hive-site.xml	2018-12-16 11:17:51.000000000 +0800
@@ -368,7 +368,7 @@
   </property>
   <property>
     <name>hive.metastore.uris</name>
-    <value/>
+    <value>thrift://127.0.0.1:9083</value>
     <description>Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore.</description>
   </property>
   <property>
@@ -527,7 +527,7 @@
   </property>
   <property>
     <name>javax.jdo.option.ConnectionPassword</name>
-    <value>mine</value>
+    <value>secret</value>
     <description>password to use against metastore database</description>
   </property>
   <property>
@@ -542,7 +542,7 @@
   </property>
   <property>
     <name>javax.jdo.option.ConnectionURL</name>
-    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
+    <value>jdbc:postgresql://127.0.0.1/myDB?ssl=false</value>
     <description>
       JDBC connect string for a JDBC metastore.
       To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection URL.
@@ -1017,7 +1017,7 @@
   </property>
   <property>
     <name>javax.jdo.option.ConnectionDriverName</name>
-    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
+    <value>org.postgresql.Driver</value>
     <description>Driver class name for a JDBC metastore</description>
   </property>
   <property>
@@ -1042,7 +1042,7 @@
   </property>
   <property>
     <name>javax.jdo.option.ConnectionUserName</name>
-    <value>APP</value>
+    <value>king</value>
     <description>Username to use against metastore database</description>
   </property>
   <property>
```

* **start up hive metastore service**
```bash
# start hive metastore service
/apache/hive/bin/hive --service metastore
```

#### Spark
* **start up spark nodes**
```bash
cp /apache/hive/conf/hive-site.xml /apache/spark/conf/
/apache/spark/sbin/start-master.sh
/apache/spark/sbin/start-slave.sh  spark://localhost:7077
```

#### Livy
Apache Griffin need to schedule spark jobs by server, we use livy to submit our jobs.
For some issues of Livy for HiveContext, we need to download 3 files or get them from Spark lib `$SPARK_HOME/lib/`, and put them into HDFS.
```
datanucleus-api-jdo-3.2.6.jar
datanucleus-core-3.2.10.jar
datanucleus-rdbms-3.2.9.jar
```
* **update configuration**
```bash
mkdir livy/logs

# update livy/conf/livy.conf
livy.server.host = 127.0.0.1
livy.spark.master = yarn
livy.spark.deployMode = cluster
livy.repl.enableHiveContext = true
```
* **start up livy**
```bash
/apache/livy/LivyServer
```

#### Elasticsearch

You might want to create Elasticsearch index in advance, in order to set number of shards, replicas, and other settings to desired values:
```
curl -XPUT http://es:9200/griffin -d '
{
    "aliases": {},
    "mappings": {
        "accuracy": {
            "properties": {
                "name": {
                    "fields": {
                        "keyword": {
                            "ignore_above": 256,
                            "type": "keyword"
                        }
                    },
                    "type": "text"
                },
                "tmst": {
                    "type": "date"
                }
            }
        }
    },
    "settings": {
        "index": {
            "number_of_replicas": "2",
            "number_of_shards": "5"
        }
    }
}
'
```
You should also modify some configurations of Apache Griffin for your environment.

- <b>service/src/main/resources/application.properties</b>

    ```
    # Apache Griffin server port (default 8080)
    server.port = 8080
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
	# livy
	# Port Livy: 8998 Livy2:8999
	livy.uri=http://localhost:8999/batches

	# yarn url
	yarn.uri=http://localhost:8088

	
    ```

- <b>service/src/main/resources/sparkProperties.json</b>
    ```
	{
	  "file": "hdfs:///<griffin measure path>/griffin-measure.jar",
	  "className": "org.apache.griffin.measure.Application",
	  "name": "griffin",
	  "queue": "default",
	  "numExecutors": 3,
	  "executorCores": 1,
	  "driverMemory": "1g",
	  "executorMemory": "1g",
	  "conf": {
		"spark.yarn.dist.files": "hdfs:///<path to>/hive-site.xml"
	 },
	  "files": [
	  ],
	  "jars": [
	  ]
	}

    ```
    - \<griffin measure path> is the location where you should put the jar file of measure module.

- <b>service/src/main/resources/env/env_batch.json</b>

    Adjust sinks according to your requirement. At least, you will need to adjust HDFS output
    directory (hdfs:///griffin/persist by default), and Elasticsearch URL (http://es:9200/griffin/accuracy by default).
    Similar changes are required in `env_streaming.json`.

#### Compression

Griffin Service is regular Spring Boot application, so it supports all customizations from Spring Boot.
To enable output compression, the following should be added to `application.properties`:
```
server.compression.enabled=true
server.compression.mime-types=application/json,application/xml,text/html,text/xml,text/plain,application/javascript,text/css
```

#### SSL

It is possible to enable SSL encryption for api and web endpoints. To do that, you will need to prepare keystore in Spring-compatible format (for example, PKCS12), and add the following values to `application.properties`:
```
server.ssl.key-store=/path/to/keystore.p12
server.ssl.key-store-password=yourpassword
server.ssl.keyStoreType=PKCS12
server.ssl.keyAlias=your_key_alias
```

#### LDAP

The following properties are available for LDAP:
 - **ldap.url**: URL of LDAP server.
 - **ldap.email**: Arbitrary suffix added to user's login before search, can be empty string. Used when user's DN contains some common suffix, and there is no bindDN specified. In this case, string after concatenation is used as DN for sending initial bind request.
 - **ldap.searchBase**: Subtree DN to search.
 - **ldap.searchPattern**: Filter expression, substring `{0}` is replaced with user's login after ldap.email is concatenated. This expression is used to find user object in LDAP. Access is denied if filter expression did not match any users.
 - **ldap.sslSkipVerify**: Allows to disable certificate validation for secure LDAP servers.
 - **ldap.bindDN**: Optional DN of service account used for user lookup. Useful if user's DN is different than attribute used as user's login, or if users' DNs are ambiguous.
 - **ldap.bindPassword**: Optional password of bind service account.

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

After a few seconds, we can visit our default UI of Apache Griffin (by default the port of spring boot is 8080).

  ```
  http://<your IP>:8080
  ```

You can use UI following the steps [here](../ui/user-guide.md).

**Note**: The UI does not support all the backend features, to experience the advanced features you can use services directly.
