///*
//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//*/
//package org.apache.griffin.measure.data.connector.cache
//
//import java.util.concurrent.TimeUnit
//
//import org.apache.griffin.measure.cache.info.{InfoCacheInstance, TimeInfoCache}
//import org.apache.griffin.measure.config.params.user.DataCacheParam
//import org.apache.griffin.measure.result.TimeStampInfo
//import org.apache.griffin.measure.utils.{HdfsFileDumpUtil, HdfsUtil, JsonUtil, TimeUtil}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//
//import scala.util.{Success, Try}
//
//case class HiveCacheDataConnector(sqlContext: SQLContext, dataCacheParam: DataCacheParam
//                                 ) extends CacheDataConnector {
//
//  if (!sqlContext.isInstanceOf[HiveContext]) {
//    throw new Exception("hive context not prepared!")
//  }
//
//  val config = dataCacheParam.config
//  val InfoPath = "info.path"
//  val cacheInfoPath: String = config.getOrElse(InfoPath, defCacheInfoPath).toString
//
//  val newCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.new")
//  val oldCacheLock = InfoCacheInstance.genLock(s"${cacheInfoPath}.old")
//
//  val timeRangeParam: List[String] = if (dataCacheParam.timeRange != null) dataCacheParam.timeRange else Nil
//  val deltaTimeRange: (Long, Long) = (timeRangeParam ::: List("0", "0")) match {
//    case s :: e :: _ => {
//      val ns = TimeUtil.milliseconds(s) match {
//        case Some(n) if (n < 0) => n
//        case _ => 0
//      }
//      val ne = TimeUtil.milliseconds(e) match {
//        case Some(n) if (n < 0) => n
//        case _ => 0
//      }
//      (ns, ne)
//    }
//    case _ => (0, 0)
//  }
//
//  val Database = "database"
//  val database: String = config.getOrElse(Database, "").toString
//  val TableName = "table.name"
//  val tableName: String = config.get(TableName) match {
//    case Some(s: String) if (s.nonEmpty) => s
//    case _ => throw new Exception("invalid table.name!")
//  }
//  val ParentPath = "parent.path"
//  val parentPath: String = config.get(ParentPath) match {
//    case Some(s: String) => s
//    case _ => throw new Exception("invalid parent.path!")
//  }
//  val tablePath = HdfsUtil.getHdfsFilePath(parentPath, tableName)
//
//  val concreteTableName = if (dbPrefix) s"${database}.${tableName}" else tableName
//
//  val ReadyTimeInterval = "ready.time.interval"
//  val ReadyTimeDelay = "ready.time.delay"
//  val readyTimeInterval: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeInterval, "1m").toString).getOrElse(60000L)
//  val readyTimeDelay: Long = TimeUtil.milliseconds(config.getOrElse(ReadyTimeDelay, "1m").toString).getOrElse(60000L)
//
//  val TimeStampColumn: String = TimeStampInfo.key
//  val PayloadColumn: String = "payload"
//
////  type Schema = (Long, String)
//  val schema: List[(String, String)] = List(
//    (TimeStampColumn, "bigint"),
//    (PayloadColumn, "string")
//  )
//  val schemaName = schema.map(_._1)
//
////  type Partition = (Long, Long)
//  val partition: List[(String, String, String)] = List(
//    ("hr", "bigint", "hour"),
//    ("min", "bigint", "min")
//  )
//  val partitionName = partition.map(_._1)
//
//  private val fieldSep = """|"""
//  private val rowSep = """\n"""
//  private val rowSepLiteral = "\n"
//
//  private def dbPrefix(): Boolean = {
//    database.nonEmpty && !database.equals("default")
//  }
//
//  private def tableExists(): Boolean = {
//    Try {
//      if (dbPrefix) {
//        sqlContext.tables(database).filter(tableExistsSql).collect.size
//      } else {
//        sqlContext.tables().filter(tableExistsSql).collect.size
//      }
//    } match {
//      case Success(s) => s > 0
//      case _ => false
//    }
//  }
//
//  override def init(): Unit = {
//    try {
//      if (tableExists) {
//        // drop exist table
//        val dropSql = s"""DROP TABLE ${concreteTableName}"""
//        sqlContext.sql(dropSql)
//      }
//
//      val colsSql = schema.map { field =>
//        s"`${field._1}` ${field._2}"
//      }.mkString(", ")
//      val partitionsSql = partition.map { partition =>
//        s"`${partition._1}` ${partition._2}"
//      }.mkString(", ")
//      val sql = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${concreteTableName}
//                    |(${colsSql}) PARTITIONED BY (${partitionsSql})
//                    |ROW FORMAT DELIMITED
//                    |FIELDS TERMINATED BY '${fieldSep}'
//                    |LINES TERMINATED BY '${rowSep}'
//                    |STORED AS TEXTFILE
//                    |LOCATION '${tablePath}'""".stripMargin
//      sqlContext.sql(sql)
//    } catch {
//      case e: Throwable => throw e
//    }
//  }
//
//  def available(): Boolean = {
//    true
//  }
//
//  private def encode(data: Map[String, Any], ms: Long): Option[List[Any]] = {
//    try {
//      Some(schema.map { field =>
//        val (name, _) = field
//        name match {
//          case TimeStampColumn => ms
//          case PayloadColumn => JsonUtil.toJson(data)
//          case _ => null
//        }
//      })
//    } catch {
//      case _ => None
//    }
//  }
//
//  private def decode(data: List[Any], updateTimeStamp: Boolean): Option[Map[String, Any]] = {
//    val dataMap = schemaName.zip(data).toMap
//    dataMap.get(PayloadColumn) match {
//      case Some(v: String) => {
//        try {
//          val map = JsonUtil.toAnyMap(v)
//          val resMap = if (updateTimeStamp) {
//            dataMap.get(TimeStampColumn) match {
//              case Some(t) => map + (TimeStampColumn -> t)
//              case _ => map
//            }
//          } else map
//          Some(resMap)
//        } catch {
//          case _ => None
//        }
//      }
//      case _ => None
//    }
//  }
//
//  def saveData(rdd: RDD[Map[String, Any]], ms: Long): Unit = {
//    val newCacheLocked = newCacheLock.lock(-1, TimeUnit.SECONDS)
//    if (newCacheLocked) {
//      try {
//        val ptns = getPartition(ms)
//        val ptnsPath = genPartitionHdfsPath(ptns)
//        val dirPath = s"${tablePath}/${ptnsPath}"
//        val fileName = s"${ms}"
//        val filePath = HdfsUtil.getHdfsFilePath(dirPath, fileName)
//
//        // encode data
//        val dataRdd: RDD[List[Any]] = rdd.flatMap(encode(_, ms))
//
//        // save data
//        val recordRdd: RDD[String] = dataRdd.map { dt =>
//          dt.map(_.toString).mkString(fieldSep)
//        }
//
//        val dumped = if (!recordRdd.isEmpty) {
//          HdfsFileDumpUtil.dump(filePath, recordRdd, rowSepLiteral)
//        } else false
//
//        // add partition
//        if (dumped) {
//          val sql = addPartitionSql(concreteTableName, ptns)
//          sqlContext.sql(sql)
//        }
//
//        // submit ms
//        submitCacheTime(ms)
//        submitReadyTime(ms)
//      } catch {
//        case e: Throwable => error(s"save data error: ${e.getMessage}")
//      } finally {
//        newCacheLock.unlock()
//      }
//    }
//  }
//
//  def readData(): Try[RDD[Map[String, Any]]] = Try {
//    val timeRange = TimeInfoCache.getTimeRange
//    submitLastProcTime(timeRange._2)
//
//    val reviseTimeRange = (timeRange._1 + deltaTimeRange._1, timeRange._2 + deltaTimeRange._2)
//    submitCleanTime(reviseTimeRange._1)
//
//    // read directly through partition info
//    val partitionRange = getPartitionRange(reviseTimeRange._1, reviseTimeRange._2)
//    val sql = selectSql(concreteTableName, partitionRange)
//    val df = sqlContext.sql(sql)
//
//    // decode data
//    df.flatMap { row =>
//      val dt = schemaName.map { sn =>
//        row.getAs[Any](sn)
//      }
//      decode(dt, true)
//    }
//  }
//
//  override def cleanOldData(): Unit = {
//    val oldCacheLocked = oldCacheLock.lock(-1, TimeUnit.SECONDS)
//    if (oldCacheLocked) {
//      try {
//        val cleanTime = readCleanTime()
//        cleanTime match {
//          case Some(ct) => {
//            // drop partition
//            val bound = getPartition(ct)
//            val sql = dropPartitionSql(concreteTableName, bound)
//            sqlContext.sql(sql)
//          }
//          case _ => {
//            // do nothing
//          }
//        }
//      } catch {
//        case e: Throwable => error(s"clean old data error: ${e.getMessage}")
//      } finally {
//        oldCacheLock.unlock()
//      }
//    }
//  }
//
//  override def updateOldData(t: Long, oldData: Iterable[Map[String, Any]]): Unit = {
//    // parallel process different time groups, lock is unnecessary
//    val ptns = getPartition(t)
//    val ptnsPath = genPartitionHdfsPath(ptns)
//    val dirPath = s"${tablePath}/${ptnsPath}"
//    val fileName = s"${t}"
//    val filePath = HdfsUtil.getHdfsFilePath(dirPath, fileName)
//
//    try {
//      // remove out time old data
//      HdfsFileDumpUtil.remove(dirPath, fileName, true)
//
//      // save updated old data
//      if (oldData.size > 0) {
//        val recordDatas = oldData.flatMap { dt =>
//          encode(dt, t)
//        }
//        val records: Iterable[String] = recordDatas.map { dt =>
//          dt.map(_.toString).mkString(fieldSep)
//        }
//        val dumped = HdfsFileDumpUtil.dump(filePath, records, rowSepLiteral)
//      }
//    } catch {
//      case e: Throwable => error(s"update old data error: ${e.getMessage}")
//    }
//  }
//
//  override protected def genCleanTime(ms: Long): Long = {
//    val minPartition = partition.last
//    val t1 = TimeUtil.timeToUnit(ms, minPartition._3)
//    val t2 = TimeUtil.timeFromUnit(t1, minPartition._3)
//    t2
//  }
//
//  private def getPartition(ms: Long): List[(String, Any)] = {
//    partition.map { p =>
//      val (name, _, unit) = p
//      val t = TimeUtil.timeToUnit(ms, unit)
//      (name, t)
//    }
//  }
//  private def getPartitionRange(ms1: Long, ms2: Long): List[(String, (Any, Any))] = {
//    partition.map { p =>
//      val (name, _, unit) = p
//      val t1 = TimeUtil.timeToUnit(ms1, unit)
//      val t2 = TimeUtil.timeToUnit(ms2, unit)
//      (name, (t1, t2))
//    }
//  }
//
//  private def genPartitionHdfsPath(partition: List[(String, Any)]): String = {
//    partition.map(prtn => s"${prtn._1}=${prtn._2}").mkString("/")
//  }
//  private def addPartitionSql(tbn: String, partition: List[(String, Any)]): String = {
//    val partitionSql = partition.map(ptn => (s"`${ptn._1}` = ${ptn._2}")).mkString(", ")
//    val sql = s"""ALTER TABLE ${tbn} ADD IF NOT EXISTS PARTITION (${partitionSql})"""
//    sql
//  }
//  private def selectSql(tbn: String, partitionRange: List[(String, (Any, Any))]): String = {
//    val clause = partitionRange.map { pr =>
//      val (name, (r1, r2)) = pr
//      s"""`${name}` BETWEEN '${r1}' and '${r2}'"""
//    }.mkString(" AND ")
//    val whereClause = if (clause.nonEmpty) s"WHERE ${clause}" else ""
//    val sql = s"""SELECT * FROM ${tbn} ${whereClause}"""
//    sql
//  }
//  private def dropPartitionSql(tbn: String, partition: List[(String, Any)]): String = {
//    val partitionSql = partition.map(ptn => (s"PARTITION ( `${ptn._1}` < '${ptn._2}' ) ")).mkString(", ")
//    val sql = s"""ALTER TABLE ${tbn} DROP ${partitionSql}"""
//    println(sql)
//    sql
//  }
//
//  private def tableExistsSql(): String = {
//    s"tableName LIKE '${tableName}'"
//  }
//
//}
