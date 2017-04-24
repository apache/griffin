package org.apache.griffin.measure.batch.rule.expr

trait Cacheable extends DataSourceable {
  protected def cacheUnit: Boolean = false
  def cacheable(ds: String): Boolean = {
    cacheUnit && !conflict() && ((ds.isEmpty && dataSources.isEmpty) || (ds.nonEmpty && contains(ds)))
  }
  protected def getCacheExprs(ds: String): Iterable[Cacheable]

  protected def persistUnit: Boolean = false
  def persistable(ds: String): Boolean = {
    persistUnit && ((ds.isEmpty && dataSources.isEmpty) || (ds.nonEmpty && contains(ds)))
  }
  protected def getPersistExprs(ds: String): Iterable[Cacheable]
}
