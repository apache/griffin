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


ROOT_DIR=$(cd $(dirname $0); pwd)
if [ -f $ROOT_DIR/env.sh ]; then
  . $ROOT_DIR/env.sh
fi

HDFS_WORKDIR=${HDFS_WORKDIR:-/user/bark/running}
TEMP_DIR=${TEMP_DIR:-$ROOT_DIR/temp}
LOG_DIR=${LOG_DIR:-$ROOT_DIR/log}

mkdir -p $TEMP_DIR
mkdir -p $LOG_DIR

lv1tempfile=$TEMP_DIR/temp.txt
lv2tempfile=$TEMP_DIR/temp2.txt
logfile=$LOG_DIR/log.txt

set +e

hadoop fs -ls $HDFS_WORKDIR > $lv1tempfile

rm -rf $logfile
touch $logfile

while read line
do
  lv1dir=${line##* }
  echo $lv1dir
  hadoop fs -test -f $lv1dir/_START
  if [ $? -ne 0 ] && [ "${lv1dir:0:1}" == "/" ]
  then
    hadoop fs -cat $lv1dir/_watchfile > $lv2tempfile

    watchfiledone=1
    while read watchline
    do
      echo $watchline >> $logfile
      hadoop fs -test -f $watchline/_SUCCESS
      if [ $? -ne 0 ]
      then
        watchfiledone=0
      fi
    done < $lv2tempfile

    if [ $watchfiledone -eq 1 ]
    then
      hadoop fs -touchz $lv1dir/_START
      hadoop fs -test -f $lv1dir/_type_0.done
      rc1=$?
      hadoop fs -test -f $lv1dir/_type_1.done
      rc2=$?
      if [ $rc1 -eq 0 ]
      then
        echo "spark-submit --class com.ebay.bark.Accu33 --master yarn-client --queue default --executor-memory 512m --num-executors 10 accuracy-1.0-SNAPSHOT.jar  $lv1dir/cmd.txt $lv1dir/ "
        spark-submit --class com.ebay.bark.Accu33 --master yarn-client --queue default --executor-memory 512m --num-executors 10 bark-models-0.0.1-SNAPSHOT.jar  $lv1dir/cmd.txt $lv1dir/
      elif [ $rc2 -eq 0 ]
      then
        echo "spark-submit --class com.ebay.bark.Vali3 --master yarn-client --queue default --executor-memory 512m --num-executors 10 accuracy-1.0-SNAPSHOT.jar  $lv1dir/cmd.txt $lv1dir/ "
        spark-submit --class com.ebay.bark.Vali3 --master yarn-client --queue default --executor-memory 512m --num-executors 10 bark-models-0.0.1-SNAPSHOT.jar  $lv1dir/cmd.txt $lv1dir/
      fi

      echo "watch file ready" >> $logfile
      exit
    fi
  fi

done < $lv1tempfile

set -e