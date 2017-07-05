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
package org.apache.griffin.measure.rule.expr

import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

trait LiteralExpr extends Expr {
  val value: Option[Any]
  def calculateOnly(values: Map[String, Any]): Option[Any] = value
  val dataSources: Set[String] = Set.empty[String]
}

case class LiteralStringExpr(expr: String) extends LiteralExpr {
  val value: Option[String] = Some(expr)
  val desc: String = s"'${value.getOrElse("")}'"
  def dataType: DataType = StringType
}

case class LiteralNumberExpr(expr: String) extends LiteralExpr {
  val value: Option[Any] = {
    if (expr.contains(".")) {
      Try (expr.toDouble) match {
        case Success(v) => Some(v)
        case _ => throw new Exception(s"${expr} is invalid number")
      }
    } else {
      Try (expr.toLong) match {
        case Success(v) => Some(v)
        case _ => throw new Exception(s"${expr} is invalid number")
      }
    }
  }
  val desc: String = value.getOrElse("").toString
  def dataType: DataType = DoubleType
}

case class LiteralTimeExpr(expr: String) extends LiteralExpr {
  final val TimeRegex = """(\d+)(d|h|m|s|ms)""".r
  val value: Option[Long] = TimeUtil.milliseconds(expr)
  val desc: String = expr
  def dataType: DataType = LongType
}

case class LiteralBooleanExpr(expr: String) extends LiteralExpr {
  final val TrueRegex = """(?i)true""".r
  final val FalseRegex = """(?i)false""".r
  val value: Option[Boolean] = expr match {
    case TrueRegex() => Some(true)
    case FalseRegex() => Some(false)
    case _ => throw new Exception(s"${expr} is invalid boolean")
  }
  val desc: String = value.getOrElse("").toString
  def dataType: DataType = BooleanType
}

case class LiteralNullExpr(expr: String) extends LiteralExpr {
  val value: Option[Any] = Some(null)
  val desc: String = "null"
  def dataType: DataType = NullType
}

case class LiteralNoneExpr(expr: String) extends LiteralExpr {
  val value: Option[Any] = None
  val desc: String = "none"
  def dataType: DataType = NullType
}