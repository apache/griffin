package org.apache.griffin.utils

import java.net.URLDecoder

import org.apache.avro.util.Utf8

object DataTypeUtil {

  object DataTypeEnum {
    val NULL_TYPE = "null"
    val INT_TYPE = "int"
    val SHORT_TYPE = "short"
    val LONG_TYPE = "long"
    val BYTE_TYPE = "byte"
    val FLOAT_TYPE = "float"
    val DOUBLE_TYPE = "double"
    val STRING_TYPE = "string"
    val BOOLEAN_TYPE = "boolean"
    val UTF8_TYPE = "utf8"
    val ISO88591_TYPE = "iso-8859-1"

    val NULL_REGEX = """^[Nn]ull(?:Type)?$""".r
    val INT_REGEX = """^[Ii]nt(?:eger)?(?:Type)?$""".r
    val SHORT_REGEX = """^[Ss]hort(?:Type)?[Ss]mall(?:[Ii]nt)?$""".r
    val LONG_REGEX = """^[Ll]ong(?:Type)?|[Bb]ig(?:[Ii]nt)?$""".r
    val BYTE_REGEX = """^[Bb]yte(?:Type)?|[Tt]iny(?:[Ii]nt)?$""".r
    val FLOAT_REGEX = """^[Ff]loat(?:Type)?$""".r
    val DOUBLE_REGEX = """^[Dd]ouble(?:Type)?$""".r
    val STRING_REGEX = """^[Ss]tr(?:ing)?(?:Type)?|[Vv]ar(?:[Cc]har)?|[Cc]har$""".r
    val BOOLEAN_REGEX = """^[Bb]ool(?:ean)?(?:Type)?|[Bb]inary$""".r
    val UTF8_REGEX = """^[Uu][Tt][Ff](?:-)?8(?:Type)?$""".r
    val ISO88591_REGEX = """^[Ii][Ss][Oo](?:-)?8859(?:-)?1(?:Type)?$""".r
  }
  import DataTypeEnum._

  object DataTypeTuple {
    // (type name, type name pattern, type convert function, string value convert function)
    final val nullType = (NULL_TYPE, NULL_REGEX, convertNull _, StringConverter.str2Null _)
    final val intType = (INT_TYPE, INT_REGEX, convertType[Int] _, StringConverter.str2Int _)
    final val shortType = (SHORT_TYPE, SHORT_REGEX, convertType[Short] _, StringConverter.str2Short _)
    final val longType = (LONG_TYPE, LONG_REGEX, convertType[Long] _, StringConverter.str2Long _)
    final val byteType = (BYTE_TYPE, BYTE_REGEX, convertType[Byte] _, StringConverter.str2Byte _)
    final val floatType = (FLOAT_TYPE, FLOAT_REGEX, convertType[Float] _, StringConverter.str2Float _)
    final val doubleType = (DOUBLE_TYPE, DOUBLE_REGEX, convertType[Double] _, StringConverter.str2Double _)
    final val stringType = (STRING_TYPE, STRING_REGEX, convertType[String] _, StringConverter.str2String _)
    final val booleanType = (BOOLEAN_TYPE, BOOLEAN_REGEX, convertType[Boolean] _, StringConverter.str2Boolean _)
    final val utf8Type = (UTF8_TYPE, UTF8_REGEX, convertUtf8 _, StringConverter.str2Utf8 _)
    final val iso88591Type = (ISO88591_TYPE, ISO88591_REGEX, convertIso88591 _, StringConverter.str2Iso88591 _)
  }
  import DataTypeTuple._

  private def convertType[T](x: Any): T = { x.asInstanceOf[T] }
  private def convertUtf8(x: Any): String = { x.asInstanceOf[Utf8].toString }
  private def convertIso88591(x: Any): String = {
    val str = new String(x.toString.getBytes("ISO-8859-1"), "UTF-8")
    val str1 = x.asInstanceOf[Utf8].toString
    println(s"=== ${x.getClass.getSimpleName} === ${str} === ${str1} ===")
    str
  }
  private def convertNull(x: Any): Null = { null }
  private def convertDecodeString(x: Any): String = {
    val str = decodePrepare(convertType[String](x))
    URLDecoder.decode(str, "UTF-8")
  }
  private def convertDecodeUtf8(x: Any): String = {
    val str = decodePrepare(convertUtf8(x))
    URLDecoder.decode(str, "UTF-8")
  }
  private def convertDecodeIso88591(x: Any): String = {
    val str = decodePrepare(convertIso88591(x))
    URLDecoder.decode(str, "UTF-8")
  }

  def str2ConvertFunc(tp: String, decode: Boolean = false): (Any) => _ = {
    tp match {
      case nullType._2() => nullType._3
      case intType._2() => intType._3
      case shortType._2() => shortType._3
      case longType._2() => longType._3
      case byteType._2() => byteType._3
      case floatType._2() => floatType._3
      case doubleType._2() => doubleType._3
      case stringType._2() => if (decode) convertDecodeString _ else stringType._3
      case booleanType._2() => booleanType._3
      case utf8Type._2() => if (decode) convertDecodeUtf8 _ else utf8Type._3
      case iso88591Type._2() => if (decode) convertDecodeIso88591 _ else iso88591Type._3
      case _ => stringType._3
    }
  }

  def str2StrValConvertFunc(tp: String, decode: Boolean = false): (String) => _ = {
    tp match {
      case nullType._2() => nullType._4
      case intType._2() => intType._4
      case shortType._2() => shortType._4
      case longType._2() => longType._4
      case byteType._2() => byteType._4
      case floatType._2() => floatType._4
      case doubleType._2() => doubleType._4
      case stringType._2() => if (decode) StringConverter.str2DecodeString _ else stringType._4
      case booleanType._2() => booleanType._4
      case utf8Type._2() => if (decode) StringConverter.str2DecodeUtf8 _ else utf8Type._4
      case iso88591Type._2() => if (decode) StringConverter.str2DecodeIso88591 _ else iso88591Type._4
      case _ => stringType._4
    }
  }

  object StringConverter {
    def str2Null(s: String): Null = null
    def str2Int(s: String): Int = s.toInt
    def str2Short(s: String): Short = s.toShort
    def str2Long(s: String): Long = s.toLong
    def str2Byte(s: String): Byte = s.toByte
    def str2Float(s: String): Float = s.toFloat
    def str2Double(s: String): Double = s.toDouble
    def str2String(s: String): String = s
    def str2DecodeString(s: String): String = {
      val str = decodePrepare(str2String(s))
      URLDecoder.decode(str, "UTF-8")
    }
    def str2Boolean(s: String): Boolean = s.toBoolean
    def str2Utf8(s: String): String = { new Utf8(s).toString }
    def str2DecodeUtf8(s: String): String = {
      val str = decodePrepare(str2Utf8(s))
      URLDecoder.decode(str, "UTF-8")
    }
    def str2Iso88591(s: String): String = { new String(s.getBytes("ISO-8859-1"), "UTF-8") }
    def str2DecodeIso88591(s: String): String = {
      val str = decodePrepare(str2Iso88591(s))
      URLDecoder.decode(str, "UTF-8")
    }
  }

  private def decodePrepare(s: String): String = {
    s.replaceAll("%(?![0-9a-fA-F]{2})", "%25")
  }

//  object ParamTypeTuple {
//
//    final val intParamType = (INT_TYPE, INT_REGEX, getInt _, getIntList _)
//    final val shortParamType = (SHORT_TYPE, SHORT_REGEX, getInt _, getIntList _)
//    final val longParamType = (LONG_TYPE, LONG_REGEX, getLong _, getLongList _)
//    final val byteParamType = (BYTE_TYPE, BYTE_REGEX, getInt _, getIntList _)
//    final val floatParamType = (FLOAT_TYPE, FLOAT_REGEX, getDouble _, getDoubleList _)
//    final val doubleParamType = (DOUBLE_TYPE, DOUBLE_REGEX, getDouble _, getDoubleList _)
//    final val stringParamType = (STRING_TYPE, STRING_REGEX, getString _, getStringList _)
//    final val booleanParamType = (BOOLEAN_TYPE, BOOLEAN_REGEX, getBoolean _, getBooleanList _)
//  }

//  import com.typesafe.config.Config
//
//  def str2GetValueFunc(tp: String): (Config => String => _) = {
//    tp match {
//      case intParamType._2() => intParamType._3
//      case shortParamType._2() => shortParamType._3
//      case longParamType._2() => longParamType._3
//      case byteParamType._2() => byteParamType._3
//      case floatParamType._2() => floatParamType._3
//      case doubleParamType._2() => doubleParamType._3
//      case stringParamType._2() => stringParamType._3
//      case booleanParamType._2() => booleanParamType._3
//      case _ => stringType._3
//    }
//  }
//
//  def str2GetValueListFunc(tp: String): (Config => String => List[_]) = {
//    tp match {
//      case intParamType._2() => intParamType._4
//      case shortParamType._2() => shortParamType._4
//      case longParamType._2() => longParamType._4
//      case byteParamType._2() => byteParamType._4
//      case floatParamType._2() => floatParamType._4
//      case doubleParamType._2() => doubleParamType._4
//      case stringParamType._2() => stringParamType._4
//      case booleanParamType._2() => booleanParamType._4
//      case _ => stringType._4
//    }
//  }
}
