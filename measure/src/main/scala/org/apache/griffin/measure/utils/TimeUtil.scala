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
package org.apache.griffin.measure.utils

import scala.util.{Failure, Success, Try}

object TimeUtil {

  final val TimeRegex = """^([+\-]?\d+)(d|h|m|s|ms)$""".r
  final val PureTimeRegex = """^([+\-]?\d+)$""".r

  def milliseconds(timeString: String): Option[Long] = {
    val value: Option[Long] = {
      Try {
        timeString match {
          case TimeRegex(time, unit) => {
            val t = time.toLong
            unit match {
              case "d" => t * 24 * 60 * 60 * 1000
              case "h" => t * 60 * 60 * 1000
              case "m" => t * 60 * 1000
              case "s" => t * 1000
              case "ms" => t
              case _ => throw new Exception(s"${timeString} is invalid time format")
            }
          }
          case PureTimeRegex(time) => {
            val t = time.toLong
            t
          }
          case _ => throw new Exception(s"${timeString} is invalid time format")
        }
      } match {
        case Success(v) => Some(v)
        case Failure(ex) => throw ex
      }
    }
    value
  }

  def timeToUnit(ms: Long, unit: String): Long = {
    unit match {
      case "ms" => ms
      case "sec" => ms / 1000
      case "min" => ms / (60 * 1000)
      case "hour" => ms / (60 * 60 * 1000)
      case "day" => ms / (24 * 60 * 60 * 1000)
      case _ => ms / (60 * 1000)
    }
  }

  def timeFromUnit(t: Long, unit: String): Long = {
    unit match {
      case "ms" => t
      case "sec" => t * 1000
      case "min" => t * 60 * 1000
      case "hour" => t * 60 * 60 * 1000
      case "day" => t * 24 * 60 * 60 * 1000
      case _ => t * 60 * 1000
    }
  }

}
