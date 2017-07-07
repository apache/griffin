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
package org.apache.griffin.measure.batch.rule

import scala.util.{Success, Try}


object CalculationUtil {

  implicit def option2CalculationValue(v: Option[_]): CalculationValue = CalculationValue(v)

  // redefine the calculation method of operators in DSL
  case class CalculationValue(value: Option[_]) extends Serializable {

    def + (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
          case (Some(v1: String), Some(v2)) => Some(v1 + v2.toString)
          case (Some(v1: Byte), Some(v2)) => Some(v1 + v2.toString.toByte)
          case (Some(v1: Short), Some(v2)) => Some(v1 + v2.toString.toShort)
          case (Some(v1: Int), Some(v2)) => Some(v1 + v2.toString.toInt)
          case (Some(v1: Long), Some(v2)) => Some(v1 + v2.toString.toLong)
          case (Some(v1: Float), Some(v2)) => Some(v1 + v2.toString.toFloat)
          case (Some(v1: Double), Some(v2)) => Some(v1 + v2.toString.toDouble)
          case _ => value
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def - (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
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
        case _ => None
      }
    }

    def * (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
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
        case _ => None
      }
    }

    def / (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
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
        case _ => None
      }
    }

    def % (other: Option[_]): Option[_] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
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
        case _ => None
      }
    }

    def unary_- (): Option[_] = {
      value match {
        case None => None
        case Some(null) => None
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
        case (None, None) => Some(true)
        case (Some(v1), Some(v2)) => Some(v1 == v2)
        case _ => Some(false)
      }
    }

    def =!= (other: Option[_]): Option[Boolean] = {
      (value, other) match {
        case (None, None) => Some(false)
        case (Some(v1), Some(v2)) => Some(v1 != v2)
        case _ => Some(true)
      }
    }

    def > (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
          case (Some(v1: String), Some(v2: String)) => Some(v1 > v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 > v2.toString.toDouble)
          case _ => None
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def >= (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (None, None) | (Some(null), Some(null)) => Some(true)
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
          case (Some(v1: String), Some(v2: String)) => Some(v1 >= v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 >= v2.toString.toDouble)
          case _ => None
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def < (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
          case (Some(v1: String), Some(v2: String)) => Some(v1 < v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 < v2.toString.toDouble)
          case _ => None
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }

    def <= (other: Option[_]): Option[Boolean] = {
      Try {
        (value, other) match {
          case (None, None) | (Some(null), Some(null)) => Some(true)
          case (None, _) | (_, None) => None
          case (Some(null), _) | (_, Some(null)) => None
          case (Some(v1: String), Some(v2: String)) => Some(v1 <= v2)
          case (Some(v1: Byte), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Short), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Int), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Long), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Float), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case (Some(v1: Double), Some(v2)) => Some(v1 <= v2.toString.toDouble)
          case _ => None
        }
      } match {
        case Success(opt) => opt
        case _ => None
      }
    }


    def in (other: Iterable[Option[_]]): Option[Boolean] = {
      other.foldLeft(Some(false): Option[Boolean]) { (res, next) =>
        optOr(res, ===(next))
      }
    }

    def not_in (other: Iterable[Option[_]]): Option[Boolean] = {
      other.foldLeft(Some(true): Option[Boolean]) { (res, next) =>
        optAnd(res, =!=(next))
      }
    }

    def between (other: Iterable[Option[_]]): Option[Boolean] = {
      if (other.size < 2) None else {
        val (begin, end) = (other.head, other.tail.head)
        if (begin.isEmpty && end.isEmpty) Some(value.isEmpty)
        else optAnd(>=(begin), <=(end))
      }
    }

    def not_between (other: Iterable[Option[_]]): Option[Boolean] = {
      if (other.size < 2) None else {
        val (begin, end) = (other.head, other.tail.head)
        if (begin.isEmpty && end.isEmpty) Some(value.nonEmpty)
        else optOr(<(begin), >(end))
      }
    }

    def unary_! (): Option[Boolean] = {
      optNot(value)
    }

    def && (other: Option[_]): Option[Boolean] = {
      optAnd(value, other)
    }

    def || (other: Option[_]): Option[Boolean] = {
      optOr(value, other)
    }


    private def optNot(a: Option[_]): Option[Boolean] = {
      a match {
        case None => None
        case Some(null) => None
        case Some(v: Boolean) => Some(!v)
        case _ => None
      }
    }
    private def optAnd(a: Option[_], b: Option[_]): Option[Boolean] = {
      (a, b) match {
        case (None, _) | (_, None) => None
        case (Some(null), _) | (_, Some(null)) => None
        case (Some(false), _) | (_, Some(false)) => Some(false)
        case (Some(b1: Boolean), Some(b2: Boolean)) => Some(b1 && b2)
        case _ => None
      }
    }
    private def optOr(a: Option[_], b: Option[_]): Option[Boolean] = {
      (a, b) match {
        case (None, _) | (_, None) => None
        case (Some(null), _) | (_, Some(null)) => None
        case (Some(true), _) | (_, Some(true)) => Some(true)
        case (Some(b1: Boolean), Some(b2: Boolean)) => Some(b1 || b2)
        case _ => None
      }
    }
  }

}
