/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.utils

import scala.util.{Failure, Success, Try}

object TimeUtil {

  final val TimeRegex = """(\d+)(d|h|m|s|ms)""".r

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
          case _ => throw new Exception(s"${timeString} is invalid time format")
        }
      } match {
        case Success(v) => Some(v)
        case Failure(ex) => throw ex
      }
    }
  }

}
