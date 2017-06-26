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

echo "$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait"
$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait

sed s/HOSTNAME/$HOSTNAME/ job/env.json.template > job/env.json
hadoop fs -put job/env.json /griffin/json/

hadoop fs -mkdir -p /home/spark_conf
hadoop fs -put $HIVE_HOME/conf/hive-site.xml /home/spark_conf/
echo "spark.yarn.dist.files		hdfs:///home/spark_conf/hive-site.xml" >> $SPARK_HOME/conf/spark-defaults.conf


$SPARK_HOME/sbin/start-all.sh

nohup hive --service metastore > metastore.log &

nohup livy-server > livy.log &

service elasticsearch start

#griffin prepare
cd /root/data
nohup ./gen-hive-data.sh > hive-data.log &
nohup ./init-demo-data.sh > init-data.log &
cd /root

sed s/HOSTNAME/$HOSTNAME/ /root/service/config/application.properties.template > /root/service/config/application.properties
cd /root/service
nohup java -jar service.jar > service.log &
cd /root

/bin/bash -c "bash"
