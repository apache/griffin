#!/bin/bash


sed -i.bak s/^.*"hive-txn-schema-0.13.0.mysql.sql".*/"SOURCE \/apache\/hive\/scripts\/metastore\/upgrade\/mysql\/hive-txn-schema-0.13.0.mysql.sql;"/ /apache/hive/scripts/metastore/upgrade/mysql/hive-schema-1.2.0.mysql.sql

mysql -u root -p123456 < $GRIFFIN_HOME/mysql_init_metastore.sql

rm $GRIFFIN_HOME/mysql_init_metastore.sql
