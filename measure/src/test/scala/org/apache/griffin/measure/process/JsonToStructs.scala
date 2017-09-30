package org.apache.griffin.measure.process

import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.json.JSONOptions
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


case class JsonToStructs(
//                          schema: DataType,
//                          options: Map[String, String],
                          child: Expression)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {
  override def nullable: Boolean = true

//  def this(schema: DataType, options: Map[String, String], child: Expression) =
//    this(schema, options, child, None)

  // Used in `FunctionRegistry`
//  def this(child: Expression, schema: Expression) =
//  this(
//    schema = JsonExprUtils.validateSchemaLiteral(schema),
//    options = Map.empty[String, String],
//    child = child,
//    timeZoneId = None)
//
//  def this(child: Expression, schema: Expression, options: Expression) =
//    this(
//      schema = JsonExprUtils.validateSchemaLiteral(schema),
//      options = JsonExprUtils.convertToMapData(options),
//      child = child,
//      timeZoneId = None)
//
//  override def checkInputDataTypes(): TypeCheckResult = schema match {
//    case _: StructType | ArrayType(_: StructType, _) =>
//      super.checkInputDataTypes()
//    case _ => TypeCheckResult.TypeCheckFailure(
//      s"Input schema ${schema.simpleString} must be a struct or an array of structs.")
//  }

  override def dataType: DataType = MapType(StringType, StringType)

//  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
//    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(json: Any): Any = {
    if (json.toString.trim.isEmpty) return null

    try {
      JsonUtil.fromJson[Map[String, Any]](json.toString)
    } catch {
      case _: Throwable => null
    }
  }

  override def inputTypes: Seq[DataType] = StringType :: Nil
}
//
//object JsonExprUtils {
//
//  def validateSchemaLiteral(exp: Expression): StructType = exp match {
//    case Literal(s, StringType) => CatalystSqlParser.parseTableSchema(s.toString)
//    case e => throw new AnalysisException(s"Expected a string literal instead of $e")
//  }
//
//  def convertToMapData(exp: Expression): Map[String, String] = exp match {
//    case m: CreateMap
//      if m.dataType.acceptsType(MapType(StringType, StringType, valueContainsNull = false)) =>
//      val arrayMap = m.eval().asInstanceOf[ArrayBasedMapData]
//      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
//        key.toString -> value.toString
//      }
//    case m: CreateMap =>
//      throw new AnalysisException(
//        s"A type of keys and values in map() must be string, but got ${m.dataType}")
//    case _ =>
//      throw new AnalysisException("Must use a map() function for options")
//  }
//}