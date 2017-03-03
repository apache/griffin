package org.apache.griffin.util

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object DataTypeUtils {

  final val intType = ("int", """^[Ii]nt(?:eger)?(?:Type)?$""".r, IntegerType, RowGetFunc.getInt _)
  final val shortType = ("short", """^[Ss]hort(?:Type)?[Ss]mall(?:[Ii]nt)?$""".r, ShortType, RowGetFunc.getShort _)
  final val longType = ("long", """^[Ll]ong(?:Type)?|[Bb]ig(?:[Ii]nt)?$""".r, LongType, RowGetFunc.getLong _)
  final val byteType = ("byte", """^[Bb]yte(?:Type)?|[Tt]iny(?:[Ii]nt)?$""".r, ByteType, RowGetFunc.getByte _)
  final val floatType = ("float", """^[Ff]loat(?:Type)?$""".r, FloatType, RowGetFunc.getFloat _)
  final val doubleType = ("double", """^[Dd]ouble(?:Type)?$""".r, DoubleType, RowGetFunc.getDouble _)
  final val dateType = ("date", """^[Dd]ate(?:Type)?$""".r, DateType, RowGetFunc.getDate _)
  final val timestampType = ("timestamp", """^[Tt]ime(?:[Ss]tamp)?(?:Type)?$""".r, TimestampType, RowGetFunc.getTimestamp _)
  final val stringType = ("string", """^[Ss]tr(?:ing)?(?:Type)?|[Vv]ar(?:[Cc]har)?|[Cc]har$""".r, StringType, RowGetFunc.getString _)
  final val booleanType = ("boolean", """^[Bb]ool(?:ean)?(?:Type)?|[Bb]inary$""".r, BooleanType, RowGetFunc.getBoolean _)

  def str2DataType(tp: String): DataType = {
    tp match {
      case intType._2() => intType._3
      case shortType._2() => shortType._3
      case longType._2() => longType._3
      case byteType._2() => byteType._3
      case floatType._2() => floatType._3
      case doubleType._2() => doubleType._3
      case dateType._2() => dateType._3
      case timestampType._2() => timestampType._3
      case stringType._2() => stringType._3
      case booleanType._2() => booleanType._3
      case _ => stringType._3
    }
  }

  def str2RowGetFunc(tp: String): (Row, Int) => Any = {
    tp match {
      case intType._2() => intType._4
      case shortType._2() => shortType._4
      case longType._2() => longType._4
      case byteType._2() => byteType._4
      case floatType._2() => floatType._4
      case doubleType._2() => doubleType._4
      case dateType._2() => dateType._4
      case timestampType._2() => timestampType._4
      case stringType._2() => stringType._4
      case booleanType._2() => booleanType._4
      case _ => stringType._4
    }
  }

  def dataType2Str(dt: DataType): String = {
    dt match {
      case intType._3 => intType._1
      case shortType._3 => shortType._1
      case longType._3 => longType._1
      case byteType._3 => byteType._1
      case floatType._3 => floatType._1
      case doubleType._3 => doubleType._1
      case dateType._3 => dateType._1
      case timestampType._3 => timestampType._1
      case stringType._3 => stringType._1
      case booleanType._3 => booleanType._1
      case _ => stringType._1
    }
  }

  def dataType2RowGetFunc(dt: DataType): (Row, Int) => Any = {
    dt match {
      case intType._3 => intType._4
      case shortType._3 => shortType._4
      case longType._3 => longType._4
      case byteType._3 => byteType._4
      case floatType._3 => floatType._4
      case doubleType._3 => doubleType._4
      case dateType._3 => dateType._4
      case timestampType._3 => timestampType._4
      case stringType._3 => stringType._4
      case booleanType._3 => booleanType._4
      case _ => stringType._4
    }
  }

  def isNum(tp: String): Boolean = {
    tp match {
      case intType._2() => true
      case shortType._2() => true
      case longType._2() => true
      case byteType._2() => true
      case floatType._2() => true
      case doubleType._2() => true
      case _ => false
    }
  }

}


object RowGetFunc {
  def getInt(r: Row, col: Int) = { r.getInt(col) }
  def getShort(r: Row, col: Int) = { r.getShort(col) }
  def getLong(r: Row, col: Int) = { r.getLong(col) }
  def getByte(r: Row, col: Int) = { r.getByte(col) }
  def getFloat(r: Row, col: Int) = { r.getFloat(col) }
  def getDouble(r: Row, col: Int) = { r.getDouble(col) }
  def getDate(r: Row, col: Int) = { r.getDate(col) }
  def getTimestamp(r: Row, col: Int) = { r.getTimestamp(col) }
  def getString(r: Row, col: Int) = { r.getString(col) }
  def getBoolean(r: Row, col: Int) = { r.getBoolean(col) }
}

object DataConverter {
  def getDouble(data: Any): Double = {
    data match {
      case x: Double => x
      case x: Int => x.toDouble
      case x: Short => x.toDouble
      case x: Long => x.toDouble
      case x: Byte => x.toDouble
      case x: Float => x.toDouble
      case x: Date => x.getTime
//      case x: Timestamp => x.getTime
      case x: String => x.toDouble
      case x: Boolean => if (x) 1 else 0
      case _ => 0
    }
  }

  def getString(data: Any): String = {
    data match {
      case x: String => x
      case x: Int => x.toString
      case x: Short => x.toString
      case x: Long => x.toString
      case x: Byte => x.toString
      case x: Float => x.toString
      case x: Double => x.toString
      case x: Date => x.toString
//      case x: Timestamp => x.toString
      case x: Boolean => x.toString
      case _ => ""
    }
  }
}