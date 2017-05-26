#!/bin/bash

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


$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait


hadoop fs -mkdir -p /home/spark_conf
hadoop fs -put $HIVE_HOME/conf/hive-site.xml /home/spark_conf/
echo "spark.yarn.dist.files		hdfs:///home/spark_conf/hive-site.xml" >> $SPARK_HOME/conf/spark-defaults.conf


$SPARK_HOME/sbin/start-all.sh

nohup hive --service metastore > metastore.log &

nohup livy-server > livy.log &

service elasticsearch start

/bin/bash -c "bash"
