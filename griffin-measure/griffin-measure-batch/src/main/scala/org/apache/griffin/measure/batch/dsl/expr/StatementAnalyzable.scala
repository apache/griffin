package org.apache.griffin.measure.batch.dsl.expr


trait StatementAnalyzable extends Serializable {

  def getAssignVariables(): Iterable[VariableExpr] = Nil

  def getConditions(): Iterable[ConditionExpr] = Nil

  def getMappings(): Iterable[MappingExpr] = Nil

  def getKeyMappings(): Iterable[MappingExpr] = Nil

}
