package org.apache.griffin.measure.batch.utils

import scala.util.{Success, Try}


object CalculationUtil {

  implicit def option2CalculationValue(v: Option[_]): CalculationValue = CalculationValue(v)

  case class CalculationValue(value: Option[_]) extends Serializable {

    def + (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (Some(v1: String), Some(v2)) => Some(v1 + v2.toString)
          case (Some(v1: Byte), Some(v2)) => Some(v1 + v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 + v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 + v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 + v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 + v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 + v2.toString.toDouble)
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
          case (Some(v1: Byte), Some(v2)) => Some(v1 - v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 - v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 - v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 - v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 - v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 - v2.toString.toDouble)
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
          case (Some(v1: Byte), Some(v2)) => Some(v1 * v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 * v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 * v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 * v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 * v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 * v2.toString.toDouble)
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
          case (Some(v1: Byte), Some(v2)) => Some(v1 / v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 / v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 / v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 / v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 / v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 / v2.toString.toDouble)
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
          case (Some(v1: Byte), Some(v2)) => Some(v1 % v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 % v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 % v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 % v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 % v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 % v2.toString.toDouble)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => value
      }
    }

    def unary_- (): Option[_] = {
      value match {
        case Some(v: String) => Some(v.reverse.toString)
        case Some(v: Boolean) => Some(!v)
        case Some(v: Byte) => Some(-v)
        case Some(v: Short) => Some(-v)
        case Some(v: Int) => Some(-v)
        case Some(v: Long) => Some(-v)
        case Some(v: Float) => Some(-v)
        case Some(v: Double) => Some(-v)
        case Some(v) => Some(v)
        case _ => None
      }
    }


    def === (other: Option[_]): Option[Boolean] = {
      (value, other) match {
        case (Some(v1), Some(v2)) => Some(v1 == v2)
        case _ => Some(false)
      }
    }

    def =!= (other: Option[_]): Option[Boolean] = {
      (value, other) match {
        case (Some(v1), Some(v2)) => Some(v1 != v2)
        case _ => Some(true)
      }
    }

    def > (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (Some(v1: String), Some(v2: String)) => Some(v1 > v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case _ => Some(false)
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def >= (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (Some(v1: String), Some(v2: String)) => Some(v1 >= v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case _ => Some(false)
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def < (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (Some(v1: String), Some(v2: String)) => Some(v1 < v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case _ => Some(false)
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def <= (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (Some(v1: String), Some(v2: String)) => Some(v1 <= v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case _ => Some(false)
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }


    def in (other: Iterable[Option[_]]): Option[Boolean] = {
      Some(other.foldLeft(false) { (res, next) =>
        res || ===(next).getOrElse(false)
      })
    }

    def not_in (other: Iterable[Option[_]]): Option[Boolean] = {
      Some(other.foldLeft(true) { (res, next) =>
        res && =!=(next).getOrElse(false)
      })
    }

    def between (other: Iterable[Option[_]]): Option[Boolean] = {
      if (other.size < 2) None else {
        val (begin, end) = (other.head, other.tail.head)
        (>=(begin), <=(end)) match {
          case (Some(b1), Some(b2)) => Some(b1 && b2)
          case _ => None
        }
      }
    }

    def not_between (other: Iterable[Option[_]]): Option[Boolean] = {
      if (other.size < 2) None else {
        val (begin, end) = (other.head, other.tail.head)
        (<(begin), >(end)) match {
          case (Some(b1), Some(b2)) => Some(b1 || b2)
          case _ => None
        }
      }
    }

    def unary_! (): Option[Boolean] = {
      value match {
        case Some(v: Boolean) => Some(!v)
        case Some(v) => Some(false)
        case _ => None
      }
    }

    def && (other: Option[_]): Option[Boolean] = {
      (value, other) match {
        case (Some(b1: Boolean), Some(b2: Boolean)) => Some(b1 && b2)
        case _ => None
      }
    }

    def || (other: Option[_]): Option[Boolean] = {
      (value, other) match {
        case (Some(b1: Boolean), Some(b2: Boolean)) => Some(b1 || b2)
        case _ => None
      }
    }
  }

}
