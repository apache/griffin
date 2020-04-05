package org.apache.griffin.measure.datasource.connector.batch

import scala.collection.mutable.{Map => MutableMap}
import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.utils.ParamUtil._

case class ElasticSearchDataConnector(
    @transient sparkSession: SparkSession,
    dcParam: DataConnectorParam,
    timestampStorage: TimestampStorage)
    extends BatchDataConnector {
  val config: Map[String, Any] = dcParam.getConfig

  import ElasticSearchDataConnector._

  final val fields: Seq[String] = config.getStringArr(Fields).map(_.trim)
  final val paths: String = config.getStringArr(Paths).map(_.trim).mkString(",") match {
    case s: String if s.isEmpty =>
      griffinLogger.error(s"Mandatory configuration '$Paths' is either empty or not defined.")
      throw new IllegalArgumentException()
    case s: String => s
  }
  val options: MutableMap[String, String] = MutableMap(
    config.getParamStringMap(Options, Map.empty).toSeq: _*)

  val queryExprs: Seq[String] = config.getStringArr(QueryExprs)

  override def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = {
      val dfOpt = Try {
        val indexesDF = sparkSession.read
          .options(options)
          .format(ElasticSearchFormat)
          .load(paths)

        val df =
          if (fields.nonEmpty) indexesDF.select(fields.head, fields.tail: _*) else indexesDF

        queryExprs.foldLeft(df)((currentDf, expr) => currentDf.where(expr))
      }.toOption

      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    }

    (dfOpt, TimeRange(ms, readTmst(ms)))
  }
}

object ElasticSearchDataConnector {
  final val ElasticSearchFormat: String = "es"
  final val Paths: String = "paths"
  final val Fields: String = "fields"
  final val Options: String = "options"
  final val QueryExprs: String = "queryExprs"
}
