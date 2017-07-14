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
package org.apache.griffin.measure.rule

import org.apache.spark.sql.types._

object DataTypeCalculationUtil {

  implicit def dataType2CalculationType(tp: DataType): CalculationType = CalculationType(tp)

  case class CalculationType(tp: DataType) extends Serializable {
    def binaryOpr (other: DataType): DataType = {
      (tp, other) match {
        case (NullType, _) | (_, NullType) => NullType
        case (t, _) => t
      }
    }
    def unaryOpr (): DataType = {
      tp
    }
  }

  case class DataTypeException() extends Exception {}

  def getDataType(value: Any): DataType = {
    value match {
      case v: String => StringType
      case v: Boolean => BooleanType
      case v: Long => LongType
      case v: Int => IntegerType
      case v: Short => ShortType
      case v: Byte => ByteType
      case v: Double => DoubleType
      case v: Float => FloatType
      case v: Map[_, _] => MapType(getSameDataType(v.keys), getSameDataType(v.values))
      case v: Iterable[_] => ArrayType(getSameDataType(v))
      case _ => NullType
    }
  }

  private def getSameDataType(values: Iterable[Any]): DataType = {
    values.foldLeft(NullType: DataType)((a, c) => genericTypeOf(a, getDataType(c)))
  }

  private def genericTypeOf(dt1: DataType, dt2: DataType): DataType = {
    if (dt1 == dt2) dt1 else {
      dt1 match {
        case NullType => dt2
        case StringType => dt1
        case DoubleType => {
          dt2 match {
            case StringType => dt2
            case DoubleType | FloatType | LongType | IntegerType | ShortType | ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case FloatType => {
          dt2 match {
            case StringType | DoubleType => dt2
            case FloatType | LongType | IntegerType | ShortType | ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case LongType => {
          dt2 match {
            case StringType | DoubleType | FloatType => dt2
            case LongType | IntegerType | ShortType | ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case IntegerType => {
          dt2 match {
            case StringType | DoubleType | FloatType | LongType => dt2
            case IntegerType | ShortType | ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case ShortType => {
          dt2 match {
            case StringType | DoubleType | FloatType | LongType | IntegerType => dt2
            case ShortType | ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case ByteType => {
          dt2 match {
            case StringType | DoubleType | FloatType | LongType | IntegerType | ShortType => dt2
            case ByteType => dt1
            case _ => throw DataTypeException()
          }
        }
        case BooleanType => {
          dt2 match {
            case StringType => dt2
            case BooleanType => dt1
            case _ => throw DataTypeException()
          }
        }
        case MapType(kdt1, vdt1, _) => {
          dt2 match {
            case MapType(kdt2, vdt2, _) => MapType(genericTypeOf(kdt1, kdt2), genericTypeOf(vdt1, vdt2))
            case _ => throw DataTypeException()
          }
        }
        case ArrayType(vdt1, _) => {
          dt2 match {
            case ArrayType(vdt2, _) => ArrayType(genericTypeOf(vdt1, vdt2))
            case _ => throw DataTypeException()
          }
        }
        case _ => throw DataTypeException()
      }
    }
  }

  def sequenceDataTypeMap(aggr: Map[String, DataType], value: Map[String, Any]): Map[String, DataType] = {
    val dataTypes = value.foldLeft(Map[String, DataType]()) { (map, pair) =>
      val (k, v) = pair
      try {
        map + (k -> getDataType(v))
      } catch {
        case e: DataTypeException => map
      }
    }
    combineDataTypeMap(aggr, dataTypes)
  }

  def combineDataTypeMap(aggr1: Map[String, DataType], aggr2: Map[String, DataType]): Map[String, DataType] = {
    aggr2.foldLeft(aggr1) { (a, c) =>
      a.get(c._1) match {
        case Some(t) => {
          try {
            a + (c._1 -> genericTypeOf(t, c._2))
          } catch {
            case e: DataTypeException => a
          }
        }
        case _ => a + c
      }
    }
  }

}
