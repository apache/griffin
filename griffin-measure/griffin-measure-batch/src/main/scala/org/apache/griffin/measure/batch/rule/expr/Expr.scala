package org.apache.griffin.measure.batch.rule.expr

trait Expr extends Serializable with Describable with Cacheable with Calculatable {

  protected val _defaultId: String = ExprIdCounter.emptyId

  val _id = ExprIdCounter.genId(_defaultId)

  final def getCacheExprs(ds: String): Iterable[Expr] = {
    if (cacheable(ds)) getSubCacheExprs(ds).toList :+ this else getSubCacheExprs(ds)
  }
  protected def getSubCacheExprs(ds: String): Iterable[Expr] = Nil

  final def getPersistExprs(ds: String): Iterable[Expr] = {
    if (persistable(ds)) getSubPersistExprs(ds).toList :+ this else getSubPersistExprs(ds)
  }
  protected def getSubPersistExprs(ds: String): Iterable[Expr] = Nil

  final def calculate(values: Map[String, Any]): Option[Any] = {
    values.get(_id) match {
      case Some(v) => Some(v)
      case _ => calculateOnly(values)
    }
  }
  protected def calculateOnly(values: Map[String, Any]): Option[Any]

}

