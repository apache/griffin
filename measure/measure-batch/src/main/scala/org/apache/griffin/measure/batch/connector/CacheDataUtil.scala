package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.sql.Row

import scala.util.{Success, Try}

object CacheDataUtil {

  // for now, one expr only get one value, not supporting one expr get multiple values
  private def getCacheData(data: Option[Any], expr: Expr, cachedMap: Map[String, Any]): Option[Any] = {
    Try {
      expr match {
        case selection: SelectionExpr => {
          selection.selectors.foldLeft(data) { (dt, selector) =>
            getCacheData(dt, selector, cachedMap)
          }
        }
        case selector: IndexFieldRangeSelectExpr => {
          data match {
            case Some(row: Row) => {
              if (selector.fields.size == 1) {
                selector.fields.head match {
                  case i: IndexDesc => Some(row.getAs[Any](i.index))
                  case f: FieldDesc => Some(row.getAs[Any](f.field))
                  case _ => None
                }
              } else None
            }
            case _ => None
          }
        }
        case _ => expr.calculate(cachedMap)
      }
    } match {
      case Success(v) => v
      case _ => None
    }
  }

  def genCachedMap(data: Option[Any], expr: Expr, initialCachedMap: Map[String, Any]): Map[String, Any] = {
    val valueOpt = getCacheData(data, expr, initialCachedMap)
    if (valueOpt.nonEmpty) {
      initialCachedMap + (expr._id -> valueOpt.get)
    } else initialCachedMap
  }

  def genCachedMap(data: Option[Any], exprs: Iterable[Expr], initialCachedMap: Map[String, Any]): Map[String, Any] = {
    exprs.foldLeft(initialCachedMap) { (cachedMap, expr) =>
      CacheDataUtil.genCachedMap(None, expr, cachedMap)
    }
  }

  def filterCachedMap(exprs: Iterable[Expr], cachedMap: Map[String, Any]): Map[String, Any] = {
    exprs.foldLeft(Map[String, Any]()) { (newMap, expr) =>
      val valueOpt = expr.calculate(cachedMap)
      if (valueOpt.nonEmpty) {
        newMap + (expr._id -> valueOpt.get)
      } else newMap
    }
  }

}
