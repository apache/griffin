#!/bin/bash


cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
echo "SPARK_MASTER_WEBUI_PORT=8082" >> $SPARK_HOME/conf/spark-env.sh
