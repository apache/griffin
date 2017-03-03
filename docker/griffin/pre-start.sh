#!/bin/bash


#sleep 10 seconds
sleep 5
hdfs dfsadmin -safemode leave

#restore mongodb data
mongod -f /etc/mongod.conf
mongorestore --db unitdb0 /db/unitdb0

#create griffin env
hadoop fs -mkdir /user/griffin
hadoop fs -chmod g+w /user/griffin
hadoop fs -mkdir /user/griffin/running
hadoop fs -mkdir /user/griffin/history
hadoop fs -mkdir /user/griffin/failure
hadoop fs -chmod g+w /user/griffin/running
hadoop fs -chmod g+w /user/griffin/history
hadoop fs -chmod g+w /user/griffin/failure
