package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.databricks.spark.avro._

import scala.util.{Success, Try}
import java.nio.file.{Files, Paths}

import org.apache.griffin.measure.batch.utils.HdfsUtil

case class AvroDataConnector(sqlContext: SQLContext, config: Map[String, Any],
                             groupbyExprs: Seq[MathExpr], cacheExprs: Iterable[Expr],
                             globalCacheMap: Map[String, Any]
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
      loadDataFile.map { row =>
        // generate cache data
        val cacheData: Map[String, Any] = cacheExprs.foldLeft(globalCacheMap) { (cachedMap, expr) =>
          SelectDataUtil.genCachedMap(Some(row), expr, cachedMap)
        }

        // get groupby data
        val groupbyData: Seq[AnyRef] = groupbyExprs.flatMap { expr =>
          expr.calculate(cacheData) match {
            case Some(v) => Some(v.asInstanceOf[AnyRef])
            case _ => None
          }
        }
        val key = toTuple(groupbyData)

        (key, cacheData)
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
