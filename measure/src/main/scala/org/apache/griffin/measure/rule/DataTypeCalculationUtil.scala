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
      case v: Int => LongType
      case v: Short => LongType
      case v: Byte => LongType
      case v: Double => DoubleType
      case v: Float => DoubleType
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
        case StringType => StringType
        case DoubleType => {
          dt2 match {
            case StringType => StringType
            case DoubleType | LongType => DoubleType
            case _ => throw DataTypeException()
          }
        }
        case LongType => {
          dt2 match {
            case StringType => StringType
            case DoubleType => DoubleType
            case LongType => LongType
            case _ => throw DataTypeException()
          }
        }
        case BooleanType => {
          dt2 match {
            case StringType => StringType
            case BooleanType => BooleanType
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
