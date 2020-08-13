#!/usr/bin/env bash

#
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
#

# Logging functions
function log_info() {
  echo "[INFO] | $(date -u +"%D %T") UTC | $1"
}

function log_error() {
  echo "[ERROR] | $(date -u +"%D %T") UTC | $1"
}

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

. "${BASEDIR}/bin/griffin-env.sh"

if [ ! $# -eq 2 ]; then
  log_error "env file and dq file must be provided!"
  exit 1
fi

envFile=$1
if [ ! -f ${envFile} ];then
  log_error "Not found env file: $1"
  exit
fi
shift

dqFile=$1
if [ ! -f ${dqFile} ];then
  log_error "Not found dq file: $2"
  exit
fi
shift

cd ${BASEDIR}

export CLASSPATH=$(echo ${BASEDIR}/lib/*.jar | tr ' ' ':'):${CLASSPATH}

exec "${JAVA_HOME}"/bin/java org.apache.griffin.measure.Application ${envFile} ${dqFile}
