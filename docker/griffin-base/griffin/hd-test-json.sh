#!/bin/bash


hadoop fs -mkdir /user/test
hadoop fs -put $GRIFFIN_HOME/jsonFile/accu_config.json /user/test/
hadoop fs -put $GRIFFIN_HOME/jsonFile/vali_config.json /user/test/
hadoop fs -mkdir /user/test/output
