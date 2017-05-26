#!/bin/bash

hadoop fs -mkdir /griffin
hadoop fs -mkdir /griffin/json
hadoop fs -mkdir /griffin/persist
hadoop fs -mkdir /griffin/checkpoint

hadoop fs -mkdir /griffin/data
hadoop fs -mkdir /griffin/data/batch

#jar file
hadoop fs -put jar/griffin-measure-batch.jar /griffin/

#data

#service

#job
#hadoop fs -put job/env.json /griffin/json/
hadoop fs -put job/config.json /griffin/json/
