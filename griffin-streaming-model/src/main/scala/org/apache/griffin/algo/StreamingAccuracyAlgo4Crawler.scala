package org.apache.griffin.algo

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.griffin.algo.core.AccuracyCore
import org.apache.griffin.cache.{CacheProcesser, CachedAccuData}
import org.apache.griffin.config.params._
import org.apache.griffin.dump._
import org.apache.griffin.prep.parse.{DataParser, Json2MapParser}
import org.apache.griffin.record.{Recorder, RecorderFactory}
import org.apache.griffin.utils.HdfsUtil
import org.apache.spark.rdd.{RDD, EmptyRDD}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.griffin.record.result.{AccuResult, Result}


case class StreamingAccuracyAlgo4Crawler( val allParam: AllParam
                                        ) extends AccuracyAlgo {

  val sparkParam: SparkParam = allParam.sparkParam
  val sourceDataParam: DataAssetParam = allParam.dataAssetParamMap.get("source").get
  val targetDataParam: DataAssetParam = allParam.dataAssetParamMap.get("target").get

  // current dumping hour and minute
  private var dumpMin: Long = 0L

  // current checked minute
  private var checkMin: Long = 0L

  // next fire time
  private var nextFireTime: Long = 0L



  def run(): Unit = {
    val ssc = StreamingContext.getOrCreate(sparkParam.cpDir,
      ( ) => {
        try {
          createContext()
        } catch {
          case runtime: RuntimeException => {
            throw runtime
          }
        }
      })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=true, stopGracefully=true)
  }

  def createContext(): StreamingContext = {
    // create context
    val sparkConf = new SparkConf().setAppName(sparkParam.appName)
    sparkConf.setAll(sparkParam.config)

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(sparkParam.logLevel)
    val sqlContext = new HiveContext(sc)
    val interval = Seconds(sparkParam.batchInterval)
    val ssc = new StreamingContext(sc, interval)
    ssc.checkpoint(sparkParam.cpDir)

    // emptyRDD
    val emptyRdd = sc.emptyRDD[(Product, (Map[String, Any], Map[String, Any]))]

    // source data direct stream
    val sourceDStream = createDirectStream(ssc, sourceDataParam)

    // target data direct stream
    val targetDStream = createDirectStream(ssc, targetDataParam)

    // accuracy param
    val accuracyParam = allParam.dqConfigParam.accuracy
    val matchValidTime = accuracyParam.matchValidTime
    val timeRange = (getTimeMs(matchValidTime.begin, matchValidTime.unit), getTimeMs(matchValidTime.end, matchValidTime.unit))

    val mapping = accuracyParam.mapping
    // get mapping Keys and Values
    val mappingKey = mapping.filter(_.isPK)
    val mappingValue = mapping

    // retry param
    val retryParam = allParam.retryParam
    val nextRetryInterval = retryParam.nextRetryInterval * 1000
    val retryPollInterval = retryParam.interval * 1000
    val retryInterval = Seconds(retryParam.interval)
    val cacheProcesser = CacheProcesser(retryParam)

    // recorder
    val recorderFactory = RecorderFactory(allParam.recorderParam)

    //--- 1. target part ---

    // target data from kafka (crawler output)
    val targetDumpConfigParam = targetDataParam.dumpConfigParam
    val targetPrepParam = targetDataParam.prepParam
    val targetSchema = targetDataParam.schema

    // target parser
    val targetParser = Json2MapParser(targetPrepParam.parseParam.configParam)

    // create hive table for target data
    val targetDumpWrapper = DumpWrapper(List[DumpWrapperInfo]())
    val targetPartitionDef = List[(String, String)](("hr", "bigint"), ("min", "bigint"))
    val (targetCreateTableSql, targetDumpDir) = genCreateTableSql(targetDumpConfigParam, targetDumpWrapper, targetPartitionDef)
    val targetTableName = targetDumpConfigParam.tableName
//    val fieldSep = targetDumpConfigParam.fieldSep
//    val lineSep = targetDumpConfigParam.lineSep
    val targetFieldSep = ","
    val targetLineSep = "\n"
    sqlContext.sql(targetCreateTableSql)

    // target dstream dump process
    targetDStream.foreachRDD((rdd, time) => {
      val ms = time.milliseconds
      val min = getTimeToUnit(ms, "min")
      val hour = getTimeToUnit(ms, "hour")
      val partitions = List[(String, Any)](("hr", hour), ("min", min))

      val partitionPath = genPartitionHdfsPath(partitions)
      val path = s"${targetDumpDir}/${partitionPath}/${ms}"

      // parse each message
      val recordRdd: RDD[String] = rdd.flatMap { kv =>
        val msg = kv._2

        // parse rdd msg
        val res: Seq[Map[String, Any]] = targetParser.parse(msg)

        // the sequence is the same with schema sequence, no need to sort here
        val records = res.map { r =>
          r.values.mkString(targetFieldSep)
        }

        records
      }

      if (!recordRdd.isEmpty) {

        // save to hdfs
        HdfsFileDumpUtil.dump(path, recordRdd, targetLineSep)

        // update hive
        val partitionSql = genAddPartitionSql(targetTableName, partitions)
        sqlContext.sql(partitionSql)
      }

      // update dumpHourMin
      updateDumpMin(min)
    })

    //--- 2. source part ---

    // source data from kafka (crawler input)
    val sourceDumpConfigParam = sourceDataParam.dumpConfigParam
    val sourcePrepParam = sourceDataParam.prepParam
    val sourceSchema = sourceDataParam.schema
    val sourceSampleParam = sourcePrepParam.sampleParam

    // source parser
    val sourceParser = Json2MapParser(sourcePrepParam.parseParam.configParam)

    // create hive table for source data
    val sourceDumpWrapper = DumpWrapper(List[DumpWrapperInfo](TimeGroupInfo, NextFireTimeInfo, MismatchInfo, ErrorInfo))
    val sourcePartitionDef = List[(String, String)](("firetime", "bigint"))
    val (sourceCreateTableSql, sourceDumpDir) = genCreateTableSql(sourceDumpConfigParam, sourceDumpWrapper, sourcePartitionDef)
    val sourceTableName = sourceDumpConfigParam.tableName
    val sourceFieldSep = ","
    val sourceLineSep = "\n"
    sqlContext.sql(sourceCreateTableSql)

    // get source dump keys
    val sourceDumpValueKeys = sourceDumpConfigParam.schema.map(_.name)
    val sourceDumpInfoKeys = sourceDumpWrapper.wrapKeys

    // source dstream parse and compare process
    val sampleInterval = Seconds(sparkParam.sampleInterval)
    sourceDStream.foreachRDD((rdd, time) => {
      val ms = time.milliseconds

      val batchTime = new Date()
      println(s"++++++  ${batchTime}  ++++++")

      val curMin = getTimeToUnit(ms, "min")

      // get next fire time
      val cft = getNextFireTime
      val (nft, fireNow, updateHive) = if (cft <= 0) {
        initNextFireTime(ms, nextRetryInterval, retryPollInterval)
        (getNextFireTime, false, true)
      } else if (cft <= ms) {
        updateNextFireTime(nextRetryInterval)
        (getNextFireTime, true, true)
      } else (cft, false, false)

      // get check min
      val icm = getCheckMin
      val curCheckMin = if (icm <= 0) {
        updateCheckMin(curMin)
        getCheckMin
      } else icm

      // dump path
      val partitions = List[(String, Any)](("firetime", nft))
      val partitionPath = genPartitionHdfsPath(partitions)
      val dumpPath = s"${sourceDumpDir}/${partitionPath}/${ms}"
      val dumpRetryPath = s"${sourceDumpDir}/${partitionPath}/${ms}.r"

      // update hive
      if (updateHive) {
        val partitionSql = genAddPartitionSql(sourceTableName, partitions)
        sqlContext.sql(partitionSql)
      }

      // sample time process
      if (time.isMultipleOf(sampleInterval)) {
        val recorders = recorderFactory.getRecorders(ms)

        // start
        recorders.foreach(_.start)

        // sample data
        val sampleRdd = if (sourceSampleParam.needSample) {
          sampleFunc(rdd, sourceSampleParam.ratio)
        } else rdd

        val sampleCount = sampleRdd.count
        println(s"===== sampleCount: ${sampleCount} =====")

        // parse data
        if (sampleRdd.isEmpty) {
          // empty source data, do nothing here
          recorders.foreach(_.info(ms, "empty source data"))
        } else {
          // parse data
          val parseRdd: RDD[Map[String, Any]] = sampleRdd.flatMap { kv =>
            val msg = kv._2
            // parse msg
            sourceParser.parse(msg)
          }

          val parseCount = parseRdd.count
          println(s"===== parseCount: ${parseCount} =====")

          // get source data
          val sourceRdd: RDD[(Product, Map[String, Any])] = parseRdd.map { row =>
            val keys: List[AnyRef] = mappingKey.map { mp =>
              row.get(mp.sourceName) match {
                case Some(v) => v.asInstanceOf[AnyRef]
                case None => null
              }
            }
            val values: Map[String, Any] = mappingValue.map { mp =>
              val v = row.get(mp.sourceName) match {
                case Some(v) => v
                case None => null
              }
              (mp.sourceName, v)
            }.toMap
            (toTuple(keys) -> values)
          }
          val sourceCount = sourceRdd.count

          println(s"===== first time sourceCount: ${sourceCount} =====")

          // wrap source data
          val sourceWrappedRdd: RDD[(Product, (Map[String, Any], Map[String, Any]))] = sourceRdd.map { r =>
            val wrappedData = Map[String, Any](
              (TimeGroupInfo.key -> ms),
              (NextFireTimeInfo.key -> nft),
              (MismatchInfo.key -> ""),
              (ErrorInfo.key -> "")
            )
            (r._1, (r._2, wrappedData))
          }

          val (result, missRdd, matchRdd) = accu(curMin, sourceWrappedRdd)

          val missingRddCount1 = missRdd.count
          println(s"===== missingRddCount1: ${missingRddCount1} =====")

          // record result
          recorders.foreach(_.accuracyResult(ms, result))

          val missStrings = missRdd.map { row =>
            val (key, (value, info)) = row
            s"${value} [${info.getOrElse(MismatchInfo.key, "unknown")}]"
          }.collect()

          // record missing records
          recorders.foreach(_.accuracyMissingRecords(missStrings))

          // cache
          val cacheData = CachedAccuData()
          cacheData.result = result
          cacheData.curTime = ms
          cacheData.deadTime = timeRange._2 + ms
          cacheProcesser.cache(ms, cacheData)

          // dump mismatch data to hive
          if (!missRdd.isEmpty) {
            // parse each record
            val dumpMissingRdd: RDD[String] = missRdd.map { kv =>
              val (_, (dt, inf)) = kv
              val values = getDumpValues(dt, sourceDumpValueKeys)
              val infos = getDumpValues(inf, sourceDumpInfoKeys)
              (values ::: infos).mkString(sourceFieldSep)
            }

            // save to hdfs
            HdfsFileDumpUtil.dump(dumpPath, dumpMissingRdd, sourceLineSep)

          }

        }

        // finish
        recorders.foreach(_.finish)

      }

      // retry
      if (fireNow) {

        val t1 = new Date().getTime
        println("===== retrying begin =====")

        // get current path
        val partitions = List[(String, Any)](("firetime", cft))
        val partitionPath = genPartitionHdfsPath(partitions)
        val dumpedDirPath = s"${sourceDumpDir}/${partitionPath}"

        println(s"===== nft: ${nft} =====")
        println(s"===== cft: ${cft} =====")
        println(s"===== dumpedDirPath: ${dumpedDirPath} =====")

        val tt1 = new Date().getTime

        // get missing data from hive
        val partitionsRange = List(("firetime", cft))
        val selectSql = genSelectSql(sourceTableName, partitionsRange)
        val missingDataFrame: DataFrame = sqlContext.sql(selectSql)

        val tt2 = new Date().getTime
        println(s"--------------- get source data from hive time: ${tt2 - tt1} ---------------")

        // get missing data
        val missingSourceWrappedRdd: RDD[(Product, (Map[String, Any], Map[String, Any]))] = missingDataFrame.map { row =>
          val keys: List[AnyRef] = mappingKey.map { mp =>
            row.getAs[Any](mp.sourceName).asInstanceOf[AnyRef]
          }
          val values: Map[String, Any] = sourceDumpValueKeys.map { key =>
            val v = row.getAs[Any](key)
            (key, v)
          }.toMap
          val infos: Map[String, Any] = sourceDumpInfoKeys.map { key =>
            val v = row.getAs[Any](key)
            (key, v)
          }.toMap
          (toTuple(keys) -> (values, infos))
        }

        val tt3 = new Date().getTime
        println(s"--------------- wrap source data time: ${tt3 - tt2} ---------------")

        if (!missingSourceWrappedRdd.isEmpty) {
          val sourceCount = missingSourceWrappedRdd.count
          println(s"===== rerty sourceCount: ${sourceCount} =====")

          val (retryResult, stillMissingRdd, matchRdd) = accu(curCheckMin, missingSourceWrappedRdd)

          val tt4 = new Date().getTime
          println(s"--------------- accu time: ${tt4 - tt3} ---------------")

          val missingRddCount2 = stillMissingRdd.count
          println(s"===== missingRddCount2: ${missingRddCount2} =====")

          val matchRddCount2 = matchRdd.count
          println(s"===== matchRddCount2: ${matchRddCount2} =====")

          // filter still need saving rdd
          val savingRdd: RDD[(Product, (Map[String, Any], Map[String, Any]))] = stillMissingRdd.filter { row =>
            val (key, (value, info)) = row
            info.get(TimeGroupInfo.key) match {
              case Some(t: Long) => {
                cacheProcesser.getCache(t) match {
                  case Some(cache) => true
                  case _ => false
                }
              }
              case _ => false
            }
          }

          def reorgByTimeGroup(rdd: RDD[(Product, (Map[String, Any], Map[String, Any]))]
                              ): RDD[(Long, (Product, (Map[String, Any], Map[String, Any])))] = {
            rdd.flatMap { row =>
              val (key, (value, info)) = row
              val b: Option[(Long, (Product, (Map[String, Any], Map[String, Any])))] = info.get(TimeGroupInfo.key) match {
                case Some(t: Long) => Some((t, row))
                case _ => None
              }
              b
            }
          }

          // get missing results
          val missingResults = reorgByTimeGroup(stillMissingRdd)
          val missingResultsCount = missingResults.count
          println(s"===== missingResultsCount: ${missingResultsCount} =====")

          // get matched results
          val matchResults = reorgByTimeGroup(matchRdd)
          val matchResultsCount = matchResults.count
          println(s"===== matchResultsCount: ${matchResultsCount} =====")

          val groupedResults = missingResults.cogroup(matchResults)

          val tt5 = new Date().getTime
          println(s"--------------- cogroup time: ${tt5 - tt4} ---------------")

//          val groupedMatchResults: RDD[(Long, Iterable[(Product, (Map[String, Any], Map[String, Any]))])] = matchResultsCount.groupByKey()

          val groupedResultsCount = groupedResults.count
          println(s"===== groupedResultsCount: ${groupedResultsCount} =====")

          groupedResults.foreach(row => println(s"=== ${row._1}: ${row._2._1.size}, ${row._2._2.size} === ${ms} ==="))

          val retry_time = new Date().getTime

          // record
          groupedResults.foreach { rslt =>
            val (t, (missRes, matchRes)) = rslt
            val curRecorders = recorderFactory.getRecorders(t)

            cacheProcesser.getCache(t) match {
              case Some(cache) => {
                val matchCount = matchRes.size
                // need to update result
                if (matchCount > 0) {
                  val accuCache = cache.asInstanceOf[CachedAccuData]
                  val delta = AccuResult(missRes.size, (missRes.size + matchRes.size))
                  val oldResult = accuCache.result
                  val newResult = accuCache.result.updateResult(delta)

                  // different result
                  if (newResult.differsFrom(oldResult)) {
                    accuCache.result = newResult

                    // record result
                    curRecorders.foreach(_.accuracyResult(ms, newResult))

                    val missStrings = missRes.map { row =>
                      val (key, (value, info)) = row
                      s"${value} [${info.getOrElse(MismatchInfo.key, "unknown")}]"
                    }
                    // record missing records
                    curRecorders.foreach(_.accuracyMissingRecords(missStrings))

                    curRecorders.foreach(_.recordTime(ms, retry_time - t1))
                    println(s"----- record here: [${ms}, (${t}: ${newResult})] -----")
                  }
                }
              }
              case _ => {
                println(s"=== no cache of ${t}, need to clear ===")
              }
            }
          }

          val savingRddCount = savingRdd.count
          println(s"===== savingRddCount: ${savingRddCount} =====")

          val tt6 = new Date().getTime
          println(s"--------------- record and get saving data time: ${tt6 - tt5} ---------------")

          // caches refresh
          cacheProcesser.refresh(ms)

          // dump mismatch data to hive
          if (!savingRdd.isEmpty) {
            // parse each record
            val dumpMissingRdd: RDD[String] = savingRdd.map { kv =>
              val (_, (dt, inf)) = kv
              val values = getDumpValues(dt, sourceDumpValueKeys)
              val infos = getDumpValues(inf, sourceDumpInfoKeys)
              (values ::: infos).mkString(sourceFieldSep)
            }

            // save to hdfs
            HdfsFileDumpUtil.dump(dumpRetryPath, dumpMissingRdd, sourceLineSep)

            val tt7 = new Date().getTime
            println(s"--------------- dump saving data time: ${tt7 - tt6} ---------------")
          }
        } else {
          println("===== retry source data empty =====")
        }

        val tt7 = new Date().getTime

        // remove current calculated dumped datas
        HdfsUtil.deleteHdfsPath(dumpedDirPath)

        val tt8 = new Date().getTime
        println(s"--------------- remove outtime data time: ${tt8 - tt7} ---------------")

        val t2 = new Date().getTime
        println(s"===== retrying end [using ${t2 - t1} ms] =====")
      }


    })

    def accu(curMin: Long, sourceWrappedRdd: RDD[(Product, (Map[String, Any], Map[String, Any]))]
            ): (AccuResult, RDD[(Product, (Map[String, Any], Map[String, Any]))], RDD[(Product, (Map[String, Any], Map[String, Any]))]) = {
      val t1 = new Date().getTime

      // get dump min
      val dumpedMin = getDumpedMin

      val mRange = (curMin, dumpedMin)
      println(s"===== mRange: ${mRange} =====")

      val ret = if (dumpReady(curMin, dumpedMin)) {
        // get target data from hive
        val minRange = (curMin, dumpedMin)
        val partitionsRange = List(("min", minRange))
        val selectSql = genSelectSql(targetTableName, partitionsRange)
        val targetDataFrame: DataFrame = sqlContext.sql(selectSql)

        // get target data
        val targetWrappedRdd: RDD[(Product, (Map[String, Any], Map[String, Any]))] = targetDataFrame.map { row =>
          val keys: List[AnyRef] = mappingKey.map { mp =>
            row.getAs[Any](mp.targetName).asInstanceOf[AnyRef]
          }
          val values: Map[String, Any] = mappingValue.map { mp =>
            val v = row.getAs[Any](mp.targetName)
            (mp.targetName, v)
          }.toMap
          (toTuple(keys) -> (values, Map[String, Any]()))
        }

        val targetCount = targetWrappedRdd.count
        println(s"===== targetCount: ${targetCount} =====")

        val (result, missRdd, matchRdd) = if (targetWrappedRdd.isEmpty) {
          val sourceCount = sourceWrappedRdd.count
          (AccuResult(sourceCount, sourceCount), sourceWrappedRdd, emptyRdd)
        } else {
          // cogroup source and target
          val allKvs = sourceWrappedRdd.cogroup(targetWrappedRdd)

          // accuracy algorithm calculation
          val (accuResult, missingRdd, matchingRdd) = AccuracyCore.accuracy(allKvs)

          println("source count: " + accuResult.totalCount + " missed count : " + accuResult.missCount)

          val missingCount = missingRdd.count
          println(s"===== missingCount: ${missingCount} =====")

          (accuResult, missingRdd, matchingRdd)
        }

        // update check min
        updateCheckMin(dumpedMin + 1)

        (result, missRdd, matchRdd)

      } else {
        println("===== dump data not ready =====")
        val sourceCount = sourceWrappedRdd.count
        (AccuResult(sourceCount, sourceCount), sourceWrappedRdd, emptyRdd)
      }

      val t2 = new Date().getTime

      println(s"===== accu using time ${t2 - t1} ms =====")

      ret
    }

    ssc

  }

  def createDirectStream(ssc: StreamingContext, dataAssetParam: DataAssetParam): InputDStream[(String, String)] = {
    val kafkaParam = dataAssetParam.kafkaConfig
    val topics = dataAssetParam.topics.split(",").toSet
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParam,
      topics
    )
  }

  def genCreateTableSql(dumpConfigParam: DumpParam, dumpWrapper: DumpWrapper, partitionDef: List[(String, String)]): (String, String) = {
    val tableName = dumpConfigParam.tableName
    val targetDumpDir = s"${dumpConfigParam.dir}/${tableName}"

    val schema = dumpConfigParam.schema
    val schemaCols = schema.map { field =>
      (field.name, field.fieldType)
    }

    val wrapperCols = dumpWrapper.wrapSchema

    val colsSql = (schemaCols ++ wrapperCols).map(col => (s"`${col._1}` ${col._2}")).mkString(", ")

//    val partitions = dumpConfigParam.partitions
//    val partitionCols = partitions.map { field =>
//      (field.name, field.fieldType)
//    }.toMap
//    val partitionsSql = partitionCols.map(prtn => (s"`${prtn._1}` ${prtn._2}")).mkString(", ")
    val partitionsSql = partitionDef.map(prtn => (s"`${prtn._1}` ${prtn._2}")).mkString(", ")

    val sql =
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS `${tableName}`
         |(${colsSql}) PARTITIONED BY (${partitionsSql})
         |ROW FORMAT DELIMITED
         |FIELDS TERMINATED BY ','
         |LINES TERMINATED BY '\n'
         |STORED AS TEXTFILE
         |LOCATION '${targetDumpDir}'""".stripMargin
    (sql, targetDumpDir)
  }


  def genPartitionHdfsPath(partitions: List[(String, Any)]): String = {
    partitions.map(prtn => s"${prtn._1}=${prtn._2}").mkString("/")
  }

  def genAddPartitionSql(tableName: String, partitions: List[(String, Any)]): String = {
    val partitionSql = partitions.map(prtn => (s"`${prtn._1}`=${prtn._2}")).mkString(", ")
    val sql = s"""ALTER TABLE `${tableName}` ADD IF NOT EXISTS PARTITION (${partitionSql})"""
    sql
  }

  def genDropPartitionSql(tableName: String, partitions: List[(String, Any)]): String = {
    val partitionSql = partitions.map(prtn => (s"`${prtn._1}`=${prtn._2}")).mkString(", ")
    val sql = s"""ALTER TABLE `${tableName}` DROP IF EXISTS PARTITION (${partitionSql})"""
    sql
  }

  def genSelectSql(tableName: String, partitionsRange: List[(String, Any)]): String = {
    val whereClause = partitionsRange match {
      case Nil => ""
      case _ => {
        val clause = partitionsRange.map { prtn =>
          prtn match {
            case (name, range: (Any, Any)) => s"""`${name}` BETWEEN '${range._1}' and '${range._2}'"""
            case (name, value) => s"""`${name}`='${value}'"""
            case _ => ""
          }
        }.filter(_.nonEmpty).mkString(" AND ")
        s"WHERE ${clause}"
      }
    }
    val sql = s"""SELECT * FROM `${tableName}` ${whereClause}"""
    sql
  }

  def sampleFunc[T](src: RDD[T], ratio: Double, count: Int = 0): RDD[T] = {
    if (src.isEmpty) {
      println("===== empty source =====")
      src
    } else {
      val sample = src.sample(false, ratio, System.nanoTime)
      if (sample.isEmpty) {
        if (count < 5) {
          sampleFunc(src, ratio, count + 1)
        } else {
          println("===== empty sample, get source =====")
          src
        }
      } else sample
    }
  }



  def getTimeMs(t: Int, unit: String): Long = {
    val lt = t.toLong
    unit match {
      case "ms" => lt
      case "sec" => lt * 1000
      case "min" => lt * 60 * 1000
      case "hour" => lt * 60 * 60 * 1000
      case "day" => lt * 24 * 60 * 60 * 1000
      case _ => lt * 60 * 1000
    }
  }

  def getTimeToUnit(lt: Long, unit: String): Long = {
    unit match {
      case "ms" => lt
      case "sec" => lt / 1000
      case "min" => lt / (60 * 1000)
      case "hour" => lt / (60 * 60 * 1000)
      case "day" => lt / (24 * 60 * 60 * 1000)
      case _ => lt / (60 * 1000)
    }
  }

  def updateDumpMin(min: Long): Unit = {
    this.dumpMin = min
  }

  def getDumpedMin(): Long = {
    this.dumpMin - 1
  }

  def dumpReady(beginMin: Long, dumpedMin: Long): Boolean = {
    beginMin <= dumpedMin
  }


  def toTuple[A <: AnyRef](as: Seq[A]): Product = {
    val tupleClass = Class.forName("scala.Tuple" + as.size)
    tupleClass.getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]
  }

  def initNextFireTime(t: Long, nextInterval: Long, precInterval: Long): Unit = {
    val nt = nextInterval + t
    this.nextFireTime = nt / precInterval * precInterval
  }

  def updateNextFireTime(nextInterval: Long): Unit = {
    this.nextFireTime += nextInterval
  }

  def getNextFireTime(): Long = {
    this.nextFireTime
  }

  def getDumpValues(data: Map[String, Any], keys: List[String]): List[String] = {
    keys.map { k =>
      data.get(k) match {
        case Some(v) => if (v != null) v.toString else ""
        case _ => ""
      }
    }
  }

  def getStringFromSchema(datas: (Map[String, Any], Map[String, Any]), dumpConfigParam: DumpParam, dumpWrapper: DumpWrapper): List[Option[_]] = {
    val (dt, info) = datas
    val schema = dumpConfigParam.schema
    val schemaValues = schema.map { s =>
      dt.get(s.name)
    }
    val infoValues = dumpWrapper.wrapValues(info)
    schemaValues ::: infoValues
  }

  def getCheckMin(): Long = {
    this.checkMin
  }

  def updateCheckMin(min: Long): Unit = {
    this.checkMin = min
  }

}
