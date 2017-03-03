#!/bin/bash


: ${HADOOP_PREFIX:=/usr/local/hadoop}
: ${TOMCAT_HOME:=/apache/tomcat}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

# altering the core-site configuration
sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml

# setting spark defaults
echo spark.yarn.jar hdfs:///spark/spark-assembly-1.6.0-hadoop2.6.0.jar > $SPARK_HOME/conf/spark-defaults.conf
cp $SPARK_HOME/conf/metrics.properties.template $SPARK_HOME/conf/metrics.properties

service sshd start
$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_PREFIX/sbin/start-yarn.sh

#spark start
$SPARK_HOME/sbin/start-all.sh

#start mongodb
mongod -f /etc/mongod.conf

#mysql service
service mysqld start

#hive metastore service
hive --service metastore &

echo "hive metastore service"

#start script
nohup ./griffin_regular_run.sh &

echo "bark regular run"


CMD=${1:-"log"}
if [[ "$CMD" == "bash" ]];
then
	#start tomcat
	/etc/init.d/tomcat start
	/bin/bash -c "$*"
else
	${TOMCAT_HOME}/bin/catalina.sh run
fi
