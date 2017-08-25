package org.apache.griffin.measure.data.source

import org.apache.griffin.measure.data.connector.direct._
import org.apache.griffin.measure.log.Loggable
import org.apache.spark.sql.{DataFrame, SQLContext}

case class DataSource(name: String, dataConnectors: Seq[DirectDataConnector]) extends Loggable with Serializable {

  def init(): Unit = {
    data match {
      case Some(df) => {
        df.registerTempTable(name)
      }
      case None => {
        throw new Exception(s"load data source ${name} fails")
      }
    }
  }

  private def data(): Option[DataFrame] = {
    dataConnectors.flatMap { dc =>
      dc.data
    }.reduceOption(_ unionAll _)
  }

}
