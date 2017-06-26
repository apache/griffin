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
wget --no-cookies --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" \
http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz \
-O jdk8-linux-x64.tar.gz
tar -xvzf jdk8-linux-x64.tar.gz
ln -s jdk1.8.0_131 jdk

# hadoop
wget http://mirror.cogentco.com/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
tar -xvf hadoop-2.6.5.tar.gz
ln -s hadoop-2.6.5 hadoop

# scala
wget http://downloads.lightbend.com/scala/2.10.6/scala-2.10.6.tgz
tar -xvf scala-2.10.6.tgz
ln -s scala-2.10.6 scala

# spark
wget http://archive.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
tar -xvf spark-1.6.0-bin-hadoop2.6.tgz
ln -s spark-1.6.0-bin-hadoop2.6 spark

# hive
wget https://www.apache.org/dist/hive/hive-1.2.2/apache-hive-1.2.2-bin.tar.gz
tar -xvf apache-hive-1.2.2-bin.tar.gz
ln -s apache-hive-1.2.2-bin hive

# livy
wget http://archive.cloudera.com/beta/livy/livy-server-0.3.0.zip
unzip livy-server-0.3.0.zip
ln -s livy-server-0.3.0 livy

#elasticsearch
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.4.0.deb
dpkg -i elasticsearch-5.4.0.deb
update-rc.d elasticsearch defaults
