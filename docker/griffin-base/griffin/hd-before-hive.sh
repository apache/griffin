#!/bin/bash


hadoop fs -mkdir /user/hive/warehouse/users_info_src
hadoop fs -put $GRIFFIN_HOME/dataFile/users_info_src.dat /user/hive/warehouse/users_info_src/
hadoop fs -mkdir /user/hive/warehouse/users_info_target
hadoop fs -put $GRIFFIN_HOME/dataFile/users_info_target.dat /user/hive/warehouse/users_info_target/
