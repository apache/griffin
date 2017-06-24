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


#create table
hive -f create-table.hql
echo "create table done"

#current hour
cur_date=`date +%Y%m%d%H`
dt=${cur_date:0:8}
hour=${cur_date:8:2}
partition_date="dt='$dt',hour='$hour'"
sed s/PARTITION_DATE/$partition_date/ ./insert-data.hql.template > insert-data.hql
hive -f insert-data.hql
echo "insert data [$partition_date] done"

#next hours
set +e
while true
do
  cur_date=`date +%Y%m%d%H`
  next_date=`date -d "+1hour" '+%Y%m%d%H'`
  dt=${next_date:0:8}
  hour=${next_date:8:2}
  partition_date="dt='$dt',hour='$hour'"
  sed s/PARTITION_DATE/$partition_date/ ./insert-data.hql.template > insert-data.hql
  hive -f insert-data.hql
  echo "insert data [$partition_date] done"
  sleep 3600
done
set -e
