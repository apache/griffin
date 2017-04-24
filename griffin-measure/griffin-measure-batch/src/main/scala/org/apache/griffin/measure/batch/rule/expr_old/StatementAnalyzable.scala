package org.apache.griffin.measure.batch.rule.expr_old


trait StatementAnalyzable extends Serializable {

  def getAssigns(): Iterable[AssignExpr] = Nil

  def getConditions(): Iterable[ConditionExpr] = Nil

  def getMappings(): Iterable[MappingExpr] = Nil

  def getKeyMappings(): Iterable[MappingExpr] = Nil

}
