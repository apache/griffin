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
