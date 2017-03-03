#!/bin/bash


#sleep 10 seconds
sleep 5
hdfs dfsadmin -safemode leave

#create directories
hadoop fs -mkdir /tmp
hadoop fs -mkdir /user/hive
hadoop fs -mkdir /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

#prepare mongod configure
mongod -f /etc/mongod.conf
