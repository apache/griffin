package org.apache.griffin.measure.connector.test


import java.util.Date

import org.apache.griffin.measure.rule.DataTypeCalculationUtil
import org.apache.griffin.measure.utils.HdfsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}


@RunWith(classOf[JUnitRunner])
class ParquetTest extends FunSuite with Matchers with BeforeAndAfter {

  test ("parquet") {
    val conf = new SparkConf().setMaster("local[*]").setAppName("parquet")
    val sc = new SparkContext(conf)
//    sc.setLogLevel(envParam.sparkParam.logLevel)
    val sqlContext = new SQLContext(sc)
//    sqlContext = new HiveContext(sc)

    val t1 = new Date().getTime()

//    val data = (0 to 99999).toList.map(d => Map[String, Any](
//      ("name" -> s"s${d}"),
//      ("age" -> (d % 10))
//    ))

    case class Data(name: String, age: Long) {
      def getName = name
      def getAge = age
    }
    val data = (0 to 99999).toList.map(d => Data(s"s${d}", d % 10))

    val rdd = sc.parallelize(data)

//    val df = genDataFrame(sqlContext, rdd)
    val df = sqlContext.createDataFrame(rdd, classOf[Data])
    println(df.count)

    val t2 = new Date().getTime()

    df.write.partitionBy("age").parquet("hdfs://localhost/test/parq")

    val t3 = new Date().getTime()
    println(s"write time: ${t3 - t2}")

    val readDf = sqlContext.read.parquet("hdfs://localhost/test/parq")
    readDf.show()
    println(readDf.count)

    val t4 = new Date().getTime()
    println(s"read time: ${t4 - t3}")

  }

  private def genDataFrame(sqlContext: SQLContext, rdd: RDD[Map[String, Any]]): DataFrame = {
    val fields = rdd.aggregate(Map[String, DataType]())(
      DataTypeCalculationUtil.sequenceDataTypeMap, DataTypeCalculationUtil.combineDataTypeMap
    ).toList.map(f => StructField(f._1, f._2))
    val schema = StructType(fields)
    val datas: RDD[Row] = rdd.map { d =>
      val values = fields.map { field =>
        val StructField(k, dt, _, _) = field
        d.get(k) match {
          case Some(v) => v
          case _ => null
        }
      }
      Row(values: _*)
    }
    val df = sqlContext.createDataFrame(datas, schema)
    df
  }

  test ("list") {
    val conf = new SparkConf().setMaster("local[*]").setAppName("parquet")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel(envParam.sparkParam.logLevel)
    val sqlContext = new SQLContext(sc)

    val filePath = "hdfs://localhost/griffin/streaming/dump/source"

    val partitionRanges: List[(Long, Long)] = List((417007L,417007L), (25020456L,25020459L))

    println(partitionRanges)

    // list partition paths
    val partitionPaths = listPartitionPathsByRanges(filePath :: Nil, partitionRanges)

    partitionPaths.foreach(println)
  }

  private def listPartitionPathsByRanges(paths: List[String], partitionRanges: List[(Long, Long)]
                                        ): List[String] = {
    partitionRanges match {
      case Nil => paths
      case head :: tail => {
        val curPaths = listPartitionPathsByRange(paths, head)
        listPartitionPathsByRanges(curPaths, tail)
      }
    }
  }

  private def listPartitionPathsByRange(paths: List[String], partitionRange: (Long, Long)
                                       ): List[String] = {
    val (lb, ub) = partitionRange
    paths.flatMap { path =>
      val names = HdfsUtil.listSubPaths(path, "dir").toList
      names.filter { name =>
        val t = str2Long(name)
        (t > 0) && (t >= lb) && (t <= ub)
      }.map(HdfsUtil.getHdfsFilePath(path, _))
    }
  }

  private def str2Long(str: String): Long = {
    try {
      str.toLong
    } catch {
      case e: Throwable => -1
    }
  }

}
