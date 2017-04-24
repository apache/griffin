package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr_old._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.databricks.spark.avro._

import scala.util.{Success, Try}
import java.nio.file.{Files, Paths}

import org.apache.griffin.measure.batch.utils.HdfsUtil

case class AvroDataConnector(sqlContext: SQLContext, config: Map[String, Any],
                             keyExprs: Seq[DataExpr], dataExprs: Iterable[DataExpr]
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
        val keys: Seq[AnyRef] = keyExprs.flatMap { expr =>
          if (expr.args.size > 0) {
            expr.args.head match {
              case e: NumPositionExpr => Some(row.getAs[Any](e.index).asInstanceOf[AnyRef])
              case e: StringPositionExpr => Some(row.getAs[Any](e.field).asInstanceOf[AnyRef])
              case _ => None
            }
          } else None
        }
        val key = toTuple(keys)
        val values: Iterable[(String, Any)] = dataExprs.flatMap { expr =>
          if (expr.args.size > 0) {
            expr.args.head match {
              case e: NumPositionExpr => Some((expr._id, row.getAs[Any](e.index)))
              case e: StringPositionExpr => Some((expr._id, row.getAs[Any](e.field)))
              case _ => None
            }
          } else None
        }
        val value = values.toMap
        (key, value)
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
