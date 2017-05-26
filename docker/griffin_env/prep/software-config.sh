#!/bin/bash

# java 8

# hadoop

cp conf/hadoop/* hadoop/etc/hadoop/
sed s/HOSTNAME/localhost/ hadoop/etc/hadoop/core-site.xml.template > hadoop/etc/hadoop/core-site.xml
sed s/HOSTNAME/localhost/ hadoop/etc/hadoop/yarn-site.xml.template > hadoop/etc/hadoop/yarn-site.xml
sed s/HOSTNAME/localhost/ hadoop/etc/hadoop/mapred-site.xml.template > hadoop/etc/hadoop/mapred-site.xml

chmod 755 hadoop/etc/hadoop/hadoop-env.sh

# scala

# spark

cp conf/spark/* spark/conf/

# hive

cp conf/hive/* hive/conf/
sed s/HOSTNAME/localhost/ hive/conf/hive-site.xml.template > hive/conf/hive-site.xml
echo "export HADOOP_HOME=/apache/hadoop" >> hive/bin/hive-config.sh

# livy

cp conf/livy/* livy/conf/
mkdir livy/logs

# elasticsearch

cp conf/elasticsearch/elasticsearch.yml /etc/elasticsearch/
cp conf/elasticsearch/elasticsearch /etc/init.d/
chmod 755 /etc/init.d/elasticsearch
