package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.sql.Row

import scala.util.{Success, Try}

object SelectDataUtil {

  // for now, one expr only get one value, not supporting one expr get multiple values
  def getSelectData(data: Option[Any], expr: Expr, cachedMap: Map[String, Any]): Option[Any] = {
    Try {
      expr match {
        case selection: SelectionExpr => {
          selection.selectors.foldLeft(data) { (dt, selector) =>
            getSelectData(dt, selector, cachedMap)
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
        case _ => None
      }
    } match {
      case Success(v) => v
      case _ => None
    }
  }

}
