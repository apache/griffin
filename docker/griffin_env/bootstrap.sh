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


$HADOOP_HOME/etc/hadoop/hadoop-env.sh
rm /tmp/*.pid

cd $HADOOP_HOME/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

service mysql start

sed s/HOSTNAME/$HOSTNAME/ $HADOOP_HOME/etc/hadoop/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml
sed s/HOSTNAME/$HOSTNAME/ $HADOOP_HOME/etc/hadoop/yarn-site.xml.template > $HADOOP_HOME/etc/hadoop/yarn-site.xml
sed s/HOSTNAME/$HOSTNAME/ $HADOOP_HOME/etc/hadoop/mapred-site.xml.template > $HADOOP_HOME/etc/hadoop/mapred-site.xml

sed s/HOSTNAME/$HOSTNAME/ $HIVE_HOME/conf/hive-site.xml.template > $HIVE_HOME/conf/hive-site.xml

/etc/init.d/ssh start

start-dfs.sh
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver

$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave


/bin/bash -c "bash"
