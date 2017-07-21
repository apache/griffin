/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.connector

import java.util.Date

import org.apache.griffin.measure.config.params.user.{DataConnectorParam, EvaluateRuleParam}
import org.apache.griffin.measure.result._
import org.apache.griffin.measure.rule.expr.StatementExpr
import org.apache.griffin.measure.rule._
import org.apache.griffin.measure.utils.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.{Failure, Success, Try}

case class KafkaDataConnector(sqlContext: SQLContext, @transient ssc: StreamingContext, dataConnectorParam: DataConnectorParam,
                              ruleExprs: RuleExprs, constFinalExprValueMap: Map[String, Any]
                             ) extends BatchDataConnector {

  @transient val kafkaStreamingDataConnector = DataConnectorFactory.getStreamingDataConnector(ssc, dataConnectorParam) match {
    case Success(cntr) => cntr
    case Failure(ex) => throw ex
  }

  val dataCacheParam = dataConnectorParam.cache
  val cacheDataConnector = DataConnectorFactory.getCacheDataConnector(sqlContext, dataCacheParam) match {
    case Success(cntr) => cntr
    case Failure(ex) => throw ex
  }

//  val DumpDatabase = "dump.database"
//  val DumpTableName = "dump.table.name"
//  val TempTableName = "temp.table.name"
//  val TableNameRegex = """^[a-zA-Z\d][\w#@]{0,127}$""".r
//
//  val dumpDatabase = dataConnectorParam.config.getOrElse(DumpDatabase, "").toString
//  val (tempSave, useTempTable, dumpTableName, tempTableName) = {
//    val (dump, dumpName) = useTable(DumpTableName)
//    val (temp, tempName) = useTable(TempTableName)
//    if (dump) {
//      (false, dumpName, tempName)
//    } else if (temp) {
//      (true, dumpName, tempName)
//    } else throw new Exception("invalid dump table name and temporary table name!")
//  }

//  private def useTable(key: String): (Boolean, String) = {
//    dataConnectorParam.config.get(key) match {
//      case Some(name: String) => {
//        name match {
//          case TableNameRegex() => (true, name)
//          case _ => (false, name)
//        }
//      }
//      case _ => (false, "")
//    }
//  }

  def available(): Boolean = {
//    kafkaStreamingDataConnector.available && cacheDataConnector.available
    cacheDataConnector.available
  }

  override def init(): Unit = {
    cacheDataConnector.init

    val ds = kafkaStreamingDataConnector.stream match {
      case Success(dstream) => dstream
      case Failure(ex) => throw ex
    }

    ds.foreachRDD((rdd, time) => {
      val ms = time.milliseconds

      val dataInfoMap = DataInfo.cacheInfoList.map(_.defWrap).toMap + TimeStampInfo.wrap(ms)

      // parse each message
      val valueMapRdd: RDD[Map[String, Any]] = rdd.flatMap { kv =>
        val msg = kv._2

        val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(msg), ruleExprs.cacheExprs, constFinalExprValueMap)
        val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)

        finalExprValueMaps.map { vm =>
          vm ++ dataInfoMap
        }
      }

      // generate DataFrame
//      val df = genDataFrame(valueMapRdd)

      // save data frame
      cacheDataConnector.saveData(valueMapRdd, ms)
    })
  }

//  // generate DataFrame
//  // maybe we can directly use def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame
//  // to avoid generate data type by myself, just translate each value into Product
//  private def genDataFrame(rdd: RDD[Map[String, Any]]): DataFrame = {
//    val fields = rdd.aggregate(Map[String, DataType]())(
//      DataTypeCalculationUtil.sequenceDataTypeMap, DataTypeCalculationUtil.combineDataTypeMap
//    ).toList.map(f => StructField(f._1, f._2))
//    val schema = StructType(fields)
//    val datas: RDD[Row] = rdd.map { d =>
//      val values = fields.map { field =>
//        val StructField(k, dt, _, _) = field
//        d.get(k) match {
//          case Some(v) => v
//          case _ => null
//        }
//      }
//      Row(values: _*)
//    }
//    val df = sqlContext.createDataFrame(datas, schema)
//    df
//  }

  def metaData(): Try[Iterable[(String, String)]] = Try {
    Map.empty[String, String]
  }

  def data(): Try[RDD[(Product, (Map[String, Any], Map[String, Any]))]] = Try {
    cacheDataConnector.readData match {
      case Success(rdd) => {
        rdd.flatMap { row =>
          // generate cache data
//          val cacheExprValueMaps = ExprValueUtil.genExprValueMaps(Some(row), ruleExprs.cacheExprs, constFinalExprValueMap)
//          val finalExprValueMaps = ExprValueUtil.updateExprValueMaps(ruleExprs.finalCacheExprs, cacheExprValueMaps)
//          val finalExprValueMap = ruleExprs.finalCacheExprs.foldLeft(Map[String, Any]()) { (mp, expr) =>
//            mp + (expr._id -> row.get(expr._id))
//          }
          val finalExprValueMap = ruleExprs.finalCacheExprs.flatMap { expr =>
            row.get(expr._id).flatMap { d =>
              Some((expr._id, d))
            }
          }.toMap

          // data info
//          val dataInfoMap: Map[String, Any] = DataInfo.cacheInfoList.map { info =>
//            try {
//              (info.key -> row.getAs[info.T](info.key))
//            } catch {
//              case e: Throwable => info.defWrap
//            }
//          }.toMap
          val dataInfoMap: Map[String, Any] = DataInfo.cacheInfoList.map { info =>
            row.get(info.key) match {
              case Some(d) => (info.key -> d)
              case _ => info.defWrap
            }
          }.toMap

          val groupbyData: Seq[AnyRef] = ruleExprs.groupbyExprs.flatMap { expr =>
            expr.calculate(finalExprValueMap) match {
              case Some(v) => Some(v.asInstanceOf[AnyRef])
              case _ => None
            }
          }
          val key = toTuple(groupbyData)

          Some((key, (finalExprValueMap, dataInfoMap)))
        }
      }
      case Failure(ex) => throw ex
    }
  }

  override def cleanOldData(): Unit = {
    cacheDataConnector.cleanOldData
  }

  override def updateOldData(t: Long, oldData: Iterable[Map[String, Any]]): Unit = {
    if (dataConnectorParam.getMatchOnce) {
      cacheDataConnector.updateOldData(t, oldData)
    }
  }

  override def updateAllOldData(oldRdd: RDD[Map[String, Any]]): Unit = {
    if (dataConnectorParam.getMatchOnce) {
      cacheDataConnector.updateAllOldData(oldRdd)
    }
  }

//  private def dbPrefix(): Boolean = {
//    if (useTempTable) false else dumpDatabase.nonEmpty && !dumpDatabase.equals("default")
//  }
//
//  private def fullDumpTableName: String = if (dbPrefix) dumpTableName else s"${dumpDatabase}.${dumpTableName}"
//
//  private def dumpTableExists(): Boolean = {
//    (!dumpTableName.isEmpty) && {
//      Try {
//        if (dbPrefix) {
//          sqlContext.tables(dumpDatabase).filter(dumpTableExistsSql).collect.size
//        } else {
//          sqlContext.tables().filter(dumpTableExistsSql).collect.size
//        }
//      } match {
//        case Success(s) => s > 0
//        case _ => false
//      }
//    }
//  }
//
//  private def dumpTableExistsSql(): String = {
//    s"tableName LIKE '${dumpTableName}'"
//  }
//
//  private def createDumpTableSql(df: DataFrame): Unit = {
////    df.schema.
////    s"CREATE TABLE IF NOT EXISTS ${fullDumpTableName} "
//  }
//
//  private def saveTempDataFrame(df: DataFrame): Unit = {
//    df.registerTempTable()
//    s"CREATE TABLE IF NOT EXISTS ${fullTempTableName} "
//  }

  private def toTuple[A <: AnyRef](as: Seq[A]): Product = {
    if (as.size > 0) {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
    } else None
  }

}
