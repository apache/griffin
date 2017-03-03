#!/bin/bash


sleep 5
hdfs dfsadmin -safemode leave

#mysql service
service mysqld start

$GRIFFIN_HOME/mysql_secure.sh 123456 && rm $GRIFFIN_HOME/mysql_secure.sh
$GRIFFIN_HOME/mysql_init.sh && rm $GRIFFIN_HOME/mysql_init.sh

#hive metastore service
hive --service metastore &

#insert hive table
hive -f $GRIFFIN_HOME/hive-input.hql

rm $GRIFFIN_HOME/hive-input.hql
