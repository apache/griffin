package org.apache.griffin.measure.batch.rule.expr

trait DataSourceable extends Serializable {
  val dataSources: Set[String]
  protected def conflict(): Boolean = dataSources.size > 1
  def contains(ds: String): Boolean = dataSources.contains(ds)
  def dataSourceOpt: Option[String] = {
    if (dataSources.size == 1) Some(dataSources.head) else None
  }
}