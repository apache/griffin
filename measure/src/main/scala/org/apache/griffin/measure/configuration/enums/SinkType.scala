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
package org.apache.griffin.measure.configuration.enums

import scala.util.matching.Regex

/**
  * sink type
  */
sealed trait SinkType {
  val idPattern: Regex
  val desc: String
}

object SinkType {
  private val sinkTypes: List[SinkType] = List(
    ConsoleSinkType,
    HdfsSinkType,
    ElasticsearchSinkType,
    MongoSinkType,
    UnknownSinkType
  )

  def apply(ptn: String): SinkType = {
    sinkTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(UnknownSinkType)
  }

  def unapply(pt: SinkType): Option[String] = Some(pt.desc)

  def validSinkTypes(strs: Seq[String]): Seq[SinkType] = {
    val seq = strs.map(s => SinkType(s)).filter(_ != UnknownSinkType).distinct
    if (seq.size > 0) seq else Seq(ElasticsearchSinkType)
  }
}

/**
  * console sink, will sink metric in console
  */
case object ConsoleSinkType extends SinkType {
  val idPattern = "^(?i)console|log$".r
  val desc = "console"
}

/**
  * hdfs sink, will sink metric and record in hdfs
  */
case object HdfsSinkType extends SinkType {
  val idPattern = "^(?i)hdfs$".r
  val desc = "hdfs"
}

/**
  * elasticsearch sink, will sink metric in elasticsearch
  */
case object ElasticsearchSinkType extends SinkType {
  val idPattern = "^(?i)es|elasticsearch|http$".r
  val desc = "elasticsearch"
}

/**
  * mongo sink, will sink metric in mongo db
  */
case object MongoSinkType extends SinkType {
  val idPattern = "^(?i)mongo|mongodb$".r
  val desc = "distinct"
}

case object UnknownSinkType extends SinkType {
  val idPattern = "".r
  val desc = "unknown"
}
