#!/bin/bash

mkdir /data
chmod 777 /data
mkdir -p /data/hadoop-data/nn
mkdir -p /data/hadoop-data/snn
mkdir -p /data/hadoop-data/dn
mkdir -p /data/hadoop-data/tmp
mkdir -p /data/hadoop-data/mapred/system
mkdir -p /data/hadoop-data/mapred/local

mkdir -p /tmp/logs
chmod 777 /tmp/logs

#elasticsearch
mkdir /data/elasticsearch
chmod 777 /data/elasticsearch
