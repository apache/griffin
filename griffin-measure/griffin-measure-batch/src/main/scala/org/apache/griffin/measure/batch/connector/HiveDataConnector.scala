package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.{Success, Try}

case class HiveDataConnector(sqlContext: SQLContext, config: Map[String, Any],
                             groupbyExprs: Seq[MathExpr], cacheExprs: Iterable[Expr]
                            ) extends DataConnector {

  val Database = "database"
  val TableName = "table.name"
  val Partitions = "partitions"

  val database = config.getOrElse(Database, "").toString
  val tableName = config.getOrElse(TableName, "").toString
  val partitionsString = config.getOrElse(Partitions, "").toString

  val concreteTableName = if (dbPrefix) s"${database}.${tableName}" else tableName
  val partitions = partitionsString.split(";").map(s => s.split(",").map(_.trim))

  private def dbPrefix(): Boolean = {
    database.nonEmpty && !database.equals("default")
  }

  def available(): Boolean = {
    (!tableName.isEmpty) && {
      Try {
        if (dbPrefix) {
          sqlContext.tables(database).filter(tableExistsSql).collect.size
        } else {
          sqlContext.tables().filter(tableExistsSql).collect.size
        }
      } match {
        case Success(s) => s > 0
        case _ => false
      }
    }
  }

  def metaData(): Try[Iterable[(String, String)]] = {
    Try {
      val originRows = sqlContext.sql(metaDataSql).map(r => (r.getString(0), r.getString(1))).collect
      val partitionPos: Int = originRows.indexWhere(pair => pair._1.startsWith("# "))
      if (partitionPos < 0) originRows
      else originRows.take(partitionPos)
    }
  }

  // for now, one expr only get one value, not supporting one expr get multiple values
  private def getSelectData(data: Option[Any], expr: Expr, cachedMap: Map[String, Any]): Option[Any] = {
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

  def data(): Try[RDD[(Product, Map[String, Any])]] = {
    Try {
      sqlContext.sql(dataSql).map { row =>
        // generate cache data
        val cacheData: Map[String, Any] = cacheExprs.foldLeft(Map[String, Any]()) { (cachedMap, expr) =>
          val valueOpt = getSelectData(Some(row), expr, cachedMap)
          cachedMap + (expr._id -> valueOpt.getOrElse(null))
          if (valueOpt.nonEmpty) {
            cachedMap + (expr._id -> valueOpt.get)
          } else cachedMap
        }
        // fixme: ...

        // get groupby data
        groupbyExprs.flatMap()

        val keys: Seq[AnyRef] = groupbyExprs.flatMap { expr =>
          if (expr.args.size > 0) {
            expr.args.head match {
              case e: NumPositionExpr => Some(row.getAs[Any](e.index).asInstanceOf[AnyRef])
              case e: StringPositionExpr => Some(row.getAs[Any](e.field).asInstanceOf[AnyRef])
              case _ => None
            }
          } else None
        }
        val key = toTuple(keys)
        val values: Iterable[(String, Any)] = dataExprs.flatMap { expr =>
          if (expr.args.size > 0) {
            expr.args.head match {
              case e: NumPositionExpr => Some((expr._id, row.getAs[Any](e.index)))
              case e: StringPositionExpr => Some((expr._id, row.getAs[Any](e.field)))
              case _ => None
            }
          } else None
        }
        val value = values.toMap
        (key, value)
      }
    }
  }

  private def tableExistsSql(): String = {
//    s"SHOW TABLES LIKE '${concreteTableName}'"    // this is hive sql, but not work for spark sql
    s"tableName LIKE '${tableName}'"
  }

  private def metaDataSql(): String = {
    s"DESCRIBE ${concreteTableName}"
  }

  private def dataSql(): String = {
    val clauses = partitions.map { prtn =>
      val cls = prtn.mkString(" AND ")
      if (cls.isEmpty) s"SELECT * FROM ${concreteTableName}"
      else s"SELECT * FROM ${concreteTableName} WHERE ${cls}"
    }
    clauses.mkString(" UNION ALL ")
  }

  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
    if (as.size > 0) {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
    } else None
  }

}
