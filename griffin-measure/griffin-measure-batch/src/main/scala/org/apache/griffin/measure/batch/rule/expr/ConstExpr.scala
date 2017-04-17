package org.apache.griffin.measure.batch.rule.expr

import scala.util.{Success, Try}

trait ConstExpr extends Expr with Calculatable {

  val value: Any

}


case class ConstStringExpr(expression: String) extends ConstExpr {

  val value: String = expression
  def genValue(values: Map[String, Any]): Option[String] = Some(value)

}

case class ConstTimeExpr(expression: String) extends ConstExpr {

  val TimeRegex = """(\d+)(y|M|w|d|h|m|s|ms)""".r

  val value: Long = expression match {
    case TimeRegex(time, unit) => {
      val t = time.toLong
      val r = unit match {
        case "y" => t * 365 * 30 * 24 * 60 * 60 * 1000
        case "M" => t * 30 * 24 * 60 * 60 * 1000
        case "w" => t * 7 * 24 * 60 * 60 * 1000
        case "d" => t * 24 * 60 * 60 * 1000
        case "h" => t * 60 * 60 * 1000
        case "m" => t * 60 * 1000
        case "s" => t * 1000
        case "ms" => t
        case _ => t
      }
      r
    }
    case _ => 0L
  }

  def genValue(values: Map[String, Any]): Option[Long] = Some(value)

}

case class ConstNumberExpr(expression: String) extends ConstExpr {

  val value: Long = {
    Try {
      expression.toLong
    } match {
      case Success(v) => v
      case _ => 0L
    }
  }

  def genValue(values: Map[String, Any]): Option[Long] = Some(value)

}