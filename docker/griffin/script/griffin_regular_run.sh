#!/bin/bash


ROOT_DIR=$(cd $(dirname $0); pwd)

set +e
while true
do
  echo "start"
  $ROOT_DIR/griffin_jobs.sh 2>&1
  rcode=$?
  echo "end $rcode"
  rm -rf $ROOT_DIR/nohup.out
  sleep 60
done
set -e
