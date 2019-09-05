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
package org.apache.griffin.measure

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait SparkSuiteBase extends FlatSpec with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _
  @transient var checkpointDir: String = _

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()
    this.spark = SparkSession.builder
      .master("local[4]")
      .appName("Griffin Job Suite")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    sc = this.spark.sparkContext

    val spark = this.spark
    import spark.implicits._
  }

  override def afterAll() {
    try {
      SparkSession.clearActiveSession()
      if (spark != null) {
        spark.stop()
      }
      spark = null
    } finally {
      super.afterAll()
    }
  }
}
