package org.apache.griffin.measure.batch.dsl.expr

trait ConstExpr extends Expr {

}


case class ConstStringExpr(expression: String) extends ConstExpr {

  val value: String = expression

}

case class ConstTimeExpr(expression: String) extends ConstExpr {

  val TimeRegex = """(\d+)(y|M|w|d|h|m|s|ms)""".r

  val value: Long = expression match {
    case TimeRegex(time, unit) => {
      val t = time.toLong
      unit match {
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
    }
  }

}

case class ConstNumberExpr(expression: String) extends ConstExpr {

  val value: Long = expression.toLong

}