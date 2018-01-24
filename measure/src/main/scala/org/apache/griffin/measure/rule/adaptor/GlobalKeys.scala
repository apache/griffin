/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.rule.adaptor

object AccuracyKeys {
  val _source = "source"
  val _target = "target"
  val _miss = "miss"
  val _total = "total"
  val _matched = "matched"
  //  val _missRecords = "missRecords"
}

object ProfilingKeys {
  val _source = "source"
}

object UniquenessKeys {
  val _source = "source"
  val _target = "target"
  val _unique = "unique"
  val _total = "total"
  val _dup = "dup"
  val _num = "num"

  val _duplicationArray = "duplication.array"
}

object DistinctnessKeys {
  val _source = "source"
  val _target = "target"
  val _distinct = "distinct"
  val _total = "total"
  val _dup = "dup"
  val _accu_dup = "accu_dup"
  val _num = "num"

  val _duplicationArray = "duplication.array"
  val _withAccumulate = "with.accumulate"
}

object TimelinessKeys {
  val _source = "source"
  val _latency = "latency"
  val _total = "total"
  val _avg = "avg"
  val _threshold = "threshold"
  val _step = "step"
  val _count = "count"
  val _stepSize = "step.size"
}

object GlobalKeys {
  val _initRule = "init.rule"
}

object ProcessDetailsKeys {
  val _baselineDataSource = "baseline.data.source"
}