package org.apache.griffin.measure.batch.rule.expr

import scala.util.{Failure, Success, Try}

trait LiteralExpr extends Expr with Calculatable {
  val value: Option[Any]
  def calculate(values: Map[String, Any]): Option[Any] = value
  val desc: String = ""
  val dataSources: Set[String] = Set.empty[String]
}

case class LiteralStringExpr(expr: String) extends LiteralExpr {
  val value: Option[String] = Some(expr)
}

case class LiteralNumberExpr(expr: String) extends LiteralExpr {
  val value: Option[Any] = {
    Try {
      if (expr.contains(".")) expr.toDouble
      else expr.toLong
    } match {
      case Success(v) => Some(v)
      case _ => throw new Exception(s"${expr} is invalid number")
    }
  }
}

case class LiteralTimeExpr(expr: String) extends LiteralExpr {
  final val TimeRegex = """(\d+)(d|h|m|s|ms)""".r
  val value: Option[Long] = {
    Try {
      expr match {
        case TimeRegex(time, unit) => {
          val t = time.toLong
          unit match {
            case "d" => t * 24 * 60 * 60 * 1000
            case "h" => t * 60 * 60 * 1000
            case "m" => t * 60 * 1000
            case "s" => t * 1000
            case "ms" => t
            case _ => throw new Exception(s"${expr} is invalid time format")
          }
        }
        case _ => throw new Exception(s"${expr} is invalid time format")
      }
    } match {
      case Success(v) => Some(v)
      case Failure(ex) => throw ex
    }
  }
}

case class LiteralBooleanExpr(expr: String) extends LiteralExpr {
  final val TrueRegex = """(?i)true""".r
  final val FalseRegex = """(?i)false""".r
  val value: Option[Boolean] = expr match {
    case TrueRegex() => Some(true)
    case FalseRegex() => Some(false)
    case _ => throw new Exception(s"${expr} is invalid boolean")
  }
}