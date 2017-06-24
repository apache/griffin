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
