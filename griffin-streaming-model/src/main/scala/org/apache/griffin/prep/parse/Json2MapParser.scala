package org.apache.griffin.prep.parse

import org.apache.griffin.config.params._
import org.apache.griffin.utils.{DataTypeUtil, ExtractJsonUtil}

import scala.collection.mutable.MutableList
import scala.util.{Failure, Success, Try}


case class Json2MapParser(param: ParseConfigParam) extends DataParser {

  type In = String
  type Out = Map[String, Any]

  def parse(data: In): Seq[Out] = {
    val datas = Some(data) :: Nil

    val schema = param.schema
    val stepsMap = schema.map { field =>
      (field.name, field.extractSteps)
    }.toMap

    val resultList: List[Map[String, Option[_]]] = ExtractJsonUtil.extractDataListWithSchemaMap(datas, stepsMap)

    resultList.map { mp =>
      mp.map { pair =>
        val (name, opt) = pair
        val field = findFieldByName(name, schema)
        val value = field match {
          case Some(f) => parseValue(opt, f)
          case _ => opt.getOrElse(null)
        }
        (name, value)
      }
    }
  }

  private def findFieldByName(name: String, fields: List[SchemaFieldParam]): Option[SchemaFieldParam] = {
    fields.filter(_.name == name) match {
      case Nil => None
      case head :: _ => Some(head)
    }
  }

  private def parseValue(valueOpt: Option[_], fieldSchema: SchemaFieldParam): Any = {
    valueOpt match {
      case Some(v) => {
        val convertFunc = DataTypeUtil.str2ConvertFunc(fieldSchema.fieldType)
        convertFunc(v)
      }
      case _ => {
        val defValue = fieldSchema.defaultValue
        val convertFunc = DataTypeUtil.str2StrValConvertFunc(fieldSchema.fieldType)
        convertFunc(defValue)
      }
    }
  }

}
