package org.apache.griffin.measure.batch.rule.expr

trait SelectExpr extends Expr {

}

case class IndexFieldRangeSelectExpr(fields: Iterable[FieldDescOnly]) extends SelectExpr {
  val desc: String = s"[${fields.mkString(",")}]"
  val dataSources: Set[String] = Set.empty[String]
}

case class FunctionOperationExpr(func: String, args: Iterable[MathExpr]) extends SelectExpr {
  val desc: String = s".${func}(${args.map(_.desc).mkString(",")})"
  val dataSources: Set[String] = args.flatMap(_.dataSources).toSet
  override def getSubCacheExprs(ds: String): Iterable[Expr] = args.flatMap(_.getCacheExprs(ds))
  override def getSubPersistExprs(ds: String): Iterable[Expr] = args.flatMap(_.getPersistExprs(ds))
}

case class FilterSelectExpr(field: FieldDesc, compare: String, value: MathExpr) extends SelectExpr {
  val desc: String = s"[${field.desc}${compare}${value.desc}]"
  val dataSources: Set[String] = value.dataSources
  override def getSubCacheExprs(ds: String): Iterable[Expr] = value.getCacheExprs(ds)
  override def getSubPersistExprs(ds: String): Iterable[Expr] = value.getPersistExprs(ds)
}

// -- selection --
case class SelectionExpr(head: SelectionHead, selectors: Iterable[SelectExpr]) extends Expr with Calculatable {
  def calculate(values: Map[String, Any]): Option[Any] = values.get(_id)

  val desc: String = {
    val argsString = selectors.map(_.desc).mkString("")
    s"${head}${argsString}"
  }
  val dataSources: Set[String] = {
    val selectorDataSources = selectors.flatMap(_.dataSources).toSet
    selectorDataSources + head.head
  }

  override def cacheUnit: Boolean = true
  override def getSubCacheExprs(ds: String): Iterable[Expr] = {
    selectors.flatMap(_.getCacheExprs(ds))
  }

  override def persistUnit: Boolean = true
  override def getSubPersistExprs(ds: String): Iterable[Expr] = {
    selectors.flatMap(_.getPersistExprs(ds))
  }
}