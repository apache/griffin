#!/bin/bash


: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/sbin/stop-dfs.sh
$HADOOP_PREFIX/sbin/stop-yarn.sh
service sshd stop
