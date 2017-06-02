/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.databricks.spark.avro._

import scala.util.{Success, Try}
import java.nio.file.{Files, Paths}

import org.apache.griffin.measure.batch.rule.{ExprValueUtil, RuleExprs}
import org.apache.griffin.measure.batch.utils.HdfsUtil

// data connector for avro file
case class AvroDataConnector(sqlContext: SQLContext, config: Map[String, Any],
                             ruleExprs: RuleExprs, constFinalExprValueMap: Map[String, Any]
                            ) extends DataConnector {

  val FilePath = "file.path"
  val FileName = "file.name"

  val filePath = config.getOrElse(FilePath, "").toString
  val fileName = config.getOrElse(FileName, "").toString

  val concreteFileFullPath = if (pathPrefix) s"${filePath}${fileName}" else fileName

  private def pathPrefix(): Boolean = {
    filePath.nonEmpty
  }

  private def fileExist(): Boolean = {
    HdfsUtil.existPath(concreteFileFullPath)
  }

  def available(): Boolean = {
    (!concreteFileFullPath.isEmpty) && fileExist
  }

  def metaData(): Try[Iterable[(String, String)]] = {
    Try {
      val st = sqlContext.read.format("com.databricks.spark.avro").load(concreteFileFullPath).schema
      st.fields.map(f => (f.name, f.dataType.typeName))
    }
  }

  def data(): Try[RDD[(Product, Map[String, Any])]] = {
    Try {
      loadDataFile.flatMap { row =>
        // generate cache data
        val cacheExprValueMap: Map[String, Any] = ruleExprs.cacheExprs.foldLeft(constFinalExprValueMap) { (cachedMap, expr) =>
          ExprValueUtil.genExprValueMap(Some(row), expr, cachedMap)
        }
        val finalExprValueMap = ExprValueUtil.updateExprValueMap(ruleExprs.finalCacheExprs, cacheExprValueMap)

        // when clause filter data source
        val whenResult = ruleExprs.whenClauseExprOpt match {
          case Some(whenClause) => whenClause.calculate(finalExprValueMap)
          case _ => None
        }

        // get groupby data
        whenResult match {
          case Some(false) => None
          case _ => {
            val groupbyData: Seq[AnyRef] = ruleExprs.groupbyExprs.flatMap { expr =>
              expr.calculate(finalExprValueMap) match {
                case Some(v) => Some(v.asInstanceOf[AnyRef])
                case _ => None
              }
            }
            val key = toTuple(groupbyData)

            Some((key, finalExprValueMap))
          }
        }
      }
    }
  }

  private def loadDataFile() = {
    sqlContext.read.format("com.databricks.spark.avro").load(concreteFileFullPath)
  }

  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
    if (as.size > 0) {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
    } else None
  }

}
