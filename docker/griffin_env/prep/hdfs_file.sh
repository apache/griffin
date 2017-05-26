#!/bin/bash

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
