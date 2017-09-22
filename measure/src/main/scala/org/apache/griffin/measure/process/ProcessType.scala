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
package org.apache.griffin.measure.process

import scala.util.matching.Regex

sealed trait ProcessType {
  val regex: Regex
  val desc: String
}

object ProcessType {
  private val procTypes: List[ProcessType] = List(BatchProcessType, StreamingProcessType)
  def apply(ptn: String): ProcessType = {
    procTypes.filter(tp => ptn match {
      case tp.regex() => true
      case _ => false
    }).headOption.getOrElse(BatchProcessType)
  }
  def unapply(pt: ProcessType): Option[String] = Some(pt.desc)
}

final case object BatchProcessType extends ProcessType {
  val regex = """^(?i)batch$""".r
  val desc = "batch"
}

final case object StreamingProcessType extends ProcessType {
  val regex = """^(?i)streaming$""".r
  val desc = "streaming"
}