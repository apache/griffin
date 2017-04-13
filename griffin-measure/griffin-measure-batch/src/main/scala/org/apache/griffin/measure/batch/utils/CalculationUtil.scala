package org.apache.griffin.measure.batch.utils

import scala.util.{Success, Try}


object CalculationUtil {

  implicit def option2CalculationValue(v: Option[_]): CalculationValue = CalculationValue(v)

  case class CalculationValue(value: Option[_]) extends Serializable {
    def + (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(s1: String), Some(v2)) => Some(s1 + v2.toString)
          case (Some(n1: Long), Some(v2)) => Some(n1 + v2.toString.toLong)
          case (Some(d1: Double), Some(v2)) => Some(d1 + v2.toString.toDouble)
          case (None, Some(v2)) => other
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }

    def - (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(n1: Long), Some(v2)) => Some(n1 - v2.toString.toLong)
          case (Some(d1: Double), Some(v2)) => Some(d1 - v2.toString.toDouble)
          case (None, Some(n2: Long)) => Some(- n2)
          case (None, Some(d2: Double)) => Some(- d2)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }

    def * (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(s1: String), Some(n2: Int)) => Some(s1 * n2)
          case (Some(s1: String), Some(n2: Long)) => Some(s1 * n2.toInt)
          case (Some(n1: Long), Some(v2)) => Some(n1 * v2.toString.toLong)
          case (Some(d1: Double), Some(v2)) => Some(d1 * v2.toString.toDouble)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }

    def / (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(n1: Long), Some(v2)) => Some(n1 / v2.toString.toLong)
          case (Some(d1: Double), Some(v2)) => Some(d1 / v2.toString.toDouble)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }

    def % (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(n1: Long), Some(v2)) => Some(n1 % v2.toString.toLong)
          case (Some(d1: Double), Some(v2)) => Some(d1 % v2.toString.toDouble)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }
  }

}
