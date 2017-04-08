package org.apache.griffin.measure.batch.connector

import org.apache.spark.rdd.RDD

import scala.util.Try


trait DataConnector extends Serializable {

  def available(): Boolean

  def metaData(): Try[Iterable[(String, String)]]

//  def data(): Try[RDD[Map[String, Any]]]

}
