package org.apache.griffin.measure.batch.rule.expr

trait Cacheable extends DataSourceable {
  def cacheable(): Boolean = !conflict()
}
