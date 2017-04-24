package org.apache.griffin.measure.batch.rule.expr

trait DataSourceable extends Serializable {
  val dataSources: Set[String]
  protected def conflict(): Boolean = dataSources.size > 1
}