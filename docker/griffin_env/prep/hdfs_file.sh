#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# spark dir
hadoop fs -mkdir -p /home/spark_lib
hadoop fs -put $SPARK_HOME/lib/spark-assembly-1.6.0-hadoop2.6.0.jar /home/spark_lib/
hadoop fs -chmod 755 /home/spark_lib/spark-assembly-1.6.0-hadoop2.6.0.jar

# yarn dir
hadoop fs -mkdir -p /yarn-logs/logs
hadoop fs -chmod g+w /yarn-logs/logs

# hive dir
hadoop fs -mkdir /tmp
hadoop fs -mkdir /user
hadoop fs -mkdir /user/hive
hadoop fs -mkdir /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

# livy dir
hadoop fs -mkdir /livy

wget http://central.maven.org/maven2/org/datanucleus/datanucleus-rdbms/3.2.9/datanucleus-rdbms-3.2.9.jar -O /apache/datanucleus-rdbms-3.2.9.jar
wget http://central.maven.org/maven2/org/datanucleus/datanucleus-core/3.2.10/datanucleus-core-3.2.10.jar -O /apache/datanucleus-core-3.2.10.jar
wget http://central.maven.org/maven2/org/datanucleus/datanucleus-api-jdo/3.2.6/datanucleus-api-jdo-3.2.6.jar -O /apache/datanucleus-api-jdo-3.2.6.jar

hadoop fs -put /apache/datanucleus-rdbms-3.2.9.jar /livy/
hadoop fs -put /apache/datanucleus-core-3.2.10.jar /livy/
hadoop fs -put /apache/datanucleus-api-jdo-3.2.6.jar /livy/
