#!/bin/bash


ROOT_DIR=$(cd $(dirname $0); pwd)
if [ -f $ROOT_DIR/env.sh ]; then
  . $ROOT_DIR/env.sh
fi

GRIFFIN_HOME=${GRIFFIN_HOME:-$ROOT_DIR}

HDFS_WORKDIR=${HDFS_WORKDIR:-/user/griffin/running}
TEMP_DIR=${TEMP_DIR:-$ROOT_DIR/temp}
LOG_DIR=${LOG_DIR:-$ROOT_DIR/log}

mkdir -p $TEMP_DIR
mkdir -p $LOG_DIR

lv1tempfile=$TEMP_DIR/temp.txt
lv2tempfile=$TEMP_DIR/temp2.txt

set +e

hadoop fs -ls $HDFS_WORKDIR > $lv1tempfile

while read line
do
  lv1dir=${line##* }
  jobid=${lv1dir##*/}
  ts=`date +%Y%m%d%H%M%S`
  echo $ts
  logfile=$LOG_DIR/${jobid}_${ts}.dqjoblog
  rm -rf $logfile
  touch $logfile

  hadoop fs -test -f $lv1dir/_START
  rc=$?
  echo "$rc $lv1dir/_START" >> $logfile
  if [ $rc -ne 0 ] && [ "${lv1dir:0:1}" == "/" ]
  then
    lv2tempfile=${LOG_DIR}/${jobid}_watch
    rm -rf $lv2tempfile
    hadoop fs -get $lv1dir/_watchfile $lv2tempfile
    cat $lv2tempfile >> $logfile

    watchfiledone=1
    hascontent=0
    while read watchline
    do
      hascontent=1
      hadoop fs -test -f $watchline/_SUCCESS
      rcode1=$?
      hadoop fs -test -f $watchline/*done
      rcode2=$?
      echo "$rcode1 $watchline/_SUCCESS  $rcode2 $watchline/*done" >> $logfile
      if [ $rcode1 -ne 0 ] && [ $rcode2 -ne 0 ]
      then
        watchfiledone=0
        break
      fi
    done < $lv2tempfile

    if [ $watchfiledone -eq 1 ] && [ $hascontent -eq 1 ]
    then
      hadoop fs -touchz $lv1dir/_START
      hadoop fs -test -f $lv1dir/_type_0.done
      rc1=$?
      echo "$rc1 $lv1dir/_type_0.done" >> $logfile
      hadoop fs -test -f $lv1dir/_type_1.done
      rc2=$?
      echo "$rc2 $lv1dir/_type_1.done" >> $logfile
      if [ $rc1 -eq 0 ]
      then
        echo "spark-submit --class org.apache.griffin.accuracy.Accu --master yarn-client --queue default --executor-memory 1g --num-executors 4 $GRIFFIN_HOME/griffin-models.jar  $lv1dir/cmd.txt $lv1dir/ " >> $logfile
        spark-submit --class org.apache.griffin.accuracy.Accu --master yarn-client --queue default --executor-memory 1g --num-executors 4 $GRIFFIN_HOME/griffin-models.jar  $lv1dir/cmd.txt $lv1dir/ >> $logfile 2>&1
      elif [ $rc2 -eq 0 ]
      then
        echo "spark-submit --class org.apache.griffin.validility.Vali --master yarn-client --queue default --executor-memory 1g --num-executors 4 $GRIFFIN_HOME/griffin-models.jar  $lv1dir/cmd.txt $lv1dir/ " >> $logfile
        spark-submit --class org.apache.griffin.validility.Vali --master yarn-client --queue default --executor-memory 1g --num-executors 4 $GRIFFIN_HOME/griffin-models.jar  $lv1dir/cmd.txt $lv1dir/ >> $logfile 2>&1
      fi

      echo "done" >> $logfile
      exit
    fi
  fi

done < $lv1tempfile

set -e
