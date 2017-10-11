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
//package org.apache.griffin.measure.process
//
//import org.apache.griffin.measure.config.params._
//import org.apache.griffin.measure.config.params.env._
//import org.apache.griffin.measure.config.params.user._
//import org.apache.griffin.measure.config.reader.ParamReaderFactory
//import org.apache.griffin.measure.config.validator.AllParamValidator
//import org.apache.griffin.measure.log.Loggable
//import org.apache.griffin.measure.persist.PersistThreadPool
//import org.apache.griffin.measure.process.engine.DataFrameOprs
//import org.apache.griffin.measure.utils.{HdfsUtil, JsonUtil}
//import org.apache.hadoop.hive.ql.exec.UDF
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql._
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types._
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//import scala.collection.mutable.WrappedArray
//import scala.util.{Failure, Success, Try}
//
//@RunWith(classOf[JUnitRunner])
//class JsonParseTest extends FunSuite with Matchers with BeforeAndAfter with Loggable {
//
//  var sparkContext: SparkContext = _
//  var sqlContext: SQLContext = _
//
//  before {
//    val conf = new SparkConf().setAppName("test json").setMaster("local[*]")
//    sparkContext = new SparkContext(conf)
//    sparkContext.setLogLevel("WARN")
////    sqlContext = new HiveContext(sparkContext)
//    sqlContext = new SQLContext(sparkContext)
//  }
//
//  test ("json test") {
//    // 0. prepare data
////    val dt =
////      """
////        |{"name": "s1", "age": 12, "items": [1, 2, 3],
////        |"subs": [{"id": 1, "type": "seed"}, {"id": 2, "type": "frog"}],
////        |"inner": {"a": 1, "b": 2}, "jstr": "{\"s1\": \"aaa\", \"s2\": 123}"
////        |}""".stripMargin
////    val rdd0 = sparkContext.parallelize(Seq(dt)).map(Row(_))
//    val rdd0 = sparkContext.textFile("src/test/resources/input.msg").map(Row(_))
//
//    val vtp = StructField("value", StringType)
//    val df0 = sqlContext.createDataFrame(rdd0, StructType(Array(vtp)))
//    df0.registerTempTable("src")
//
////    val fromJson2Array = (s: String) => {
////      JsonUtil.fromJson[Seq[String]](s)
////    }
////    sqlContext.udf.register("from_json_to_array", fromJson2Array)
////
////    val df2 = sqlContext.sql("SELECT explode(from_json_to_array(get_json_object(value, '$.seeds'))) as value FROM src")
////    df2.printSchema
////    df2.show(10)
////    df2.registerTempTable("df2")
//
//
//
//    // 1. read from json string to extracted json row
////    val readSql = "SELECT value FROM src"
////    val df = sqlContext.sql(readSql)
////    val df = sqlContext.table("src")
////    val rdd = df.map { row =>
////      row.getAs[String]("value")
////    }
////    val df1 = sqlContext.read.json(rdd)
////    df1.printSchema
////    df1.show(10)
////    df1.registerTempTable("df1")
//    val details = Map[String, Any](("df.name" -> "src"))
//    val df1 = DataFrameOprs.fromJson(sqlContext, details)
//    df1.registerTempTable("df1")
//
//    // 2. extract json array into lines
////    val rdd2 = df1.flatMap { row =>
////      row.getAs[WrappedArray[String]]("seeds")
////    }
////    val df2 = sqlContext.read.json(rdd2)
//    val df2 = sqlContext.sql("select explode(seeds) as value from df1")
////    val tdf = sqlContext.sql("select name, age, explode(items) as item from df1")
////    tdf.registerTempTable("tdf")
////    val df2 = sqlContext.sql("select struct(name, age, item) as ttt from tdf")
//    df2.printSchema
//    df2.show(10)
//    df2.registerTempTable("df2")
//    println(df2.count)
//
//    val sql1 = "SELECT value FROM df2"
//    val df22 = sqlContext.sql(sql1)
//    val rdd22 = df22.map { row =>
//      row.getAs[String]("value")
//    }
//    import org.apache.spark.sql.functions._
//    val df23 = sqlContext.read.json(rdd22)
//    df23.registerTempTable("df23")
////    df23.withColumn("par", monotonicallyIncreasingId)
//
//    val df24 = sqlContext.sql("SELECT url, cast(get_json_object(metadata, '$.tracker.crawlRequestCreateTS') as bigint) as ts FROM df23")
//    df24.printSchema
//    df24.show(10)
//    df24.registerTempTable("df24")
//    println(df24.count)
//
////    val df25 = sqlContext.sql("select ")
//
////
////    // 3. extract json string into row
//////    val df3 = sqlContext.sql("select cast(get_json_object(metadata, '$.tracker.crawlRequestCreateTS') as bigint), url from df2")
////    val df3 = sqlContext.sql("select cast(get_json_object(get_json_object(value, '$.metadata'), '$.tracker.crawlRequestCreateTS') as bigint), get_json_object(value, '$.url') from df2")
////    df3.printSchema()
////    df3.show(10)
////    println(df3.count)
//
//
//
////    val df5 = sqlContext.sql("select get_json_object(value, '$.subs') as subs from src")
////    df5.printSchema()
////    df5.show(10)
////    df5.registerTempTable("df5")
////    val rdd5 = df5.map { row =>
////      row.getAs[String]("subs")
////    }
////    val df6 = sqlContext.read.json(rdd5)
////    df6.printSchema
////    df6.show(10)
//
//    // 2. extract json string to row
////    val df2 = sqlContext.sql("select jstr from df1")
////    val rdd2 = df2.map { row =>
////      row.getAs[String]("jstr")
////    }
////    val df22 = sqlContext.read.json(rdd2)
////    df22.printSchema
////    df22.show(100)
////    df22.registerTempTable("df2")
////
////    val df23 = sqlContext.sql("select json_tuple(jstr, 's1', 's2') from df1")
////    df23.printSchema()
////    df23.show(100)
//
//    // 3. extract json array into lines ??
//
//    // 3. flatmap from json row to json row
////    val df3 = sqlContext.sql("select explode(subs) as sub, items from df1")
////    df3.printSchema()
////    df3.show(10)
////    df3.registerTempTable("df3")
////
////    val df4 = sqlContext.sql("select explode(items) as item, sub from df3")
////    df4.printSchema()
////    df4.show(10)
//
////    sqlContext.udf.register("length", (s: WrappedArray[_]) => s.length)
//    //
//    //    val df2 = sqlContext.sql("SELECT inner from df1")
//    //    df2.registerTempTable("df2")
//    //    df2.printSchema
//    //    df2.show(100)
//
////    def children(colname: String, df: DataFrame): Array[DataFrame] = {
////      val parent = df.schema.fields.filter(_.name == colname).head
////      println(parent)
////      val fields: Array[StructField] = parent.dataType match {
////        case x: StructType => x.fields
////        case _ => Array.empty[StructField]
////      }
////      fields.map(x => col(s"$colname.${x.name}"))
//////      fields.foreach(println)
////    }
//////
////    children("inner", df2)
////
////    df2.select(children("bar", df): _*).printSchema
//
////    val df3 = sqlContext.sql("select inline(subs) from df1")
////    df3.printSchema()
////    df3.show(100)
//
////    val rdd2 = df2.flatMap { row =>
////      row.getAs[GenericRowWithSchema]("inner") :: Nil
////    }
////
////    rdd2.
//
////    val funcs = sqlContext.sql("show functions")
////    funcs.printSchema()
////    funcs.show(1000)
////
////    val desc = sqlContext.sql("describe function inline")
////    desc.printSchema()
////    desc.show(100)
//
//    //
//
//  }
//
//  test ("json test 2") {
//    val rdd0 = sparkContext.textFile("src/test/resources/output.msg").map(Row(_))
//
//    val vtp = StructField("value", StringType)
//    val df0 = sqlContext.createDataFrame(rdd0, StructType(Array(vtp)))
//    df0.registerTempTable("tgt")
//
////    val fromJson2StringArray = (s: String) => {
////      val seq = JsonUtil.fromJson[Seq[Any]](s)
////      seq.map(i => JsonUtil.toJson(i))
////    }
////    sqlContext.udf.register("from_json_to_string_array", fromJson2StringArray)
////
////    val df2 = sqlContext.sql("SELECT from_json_to_string_array(get_json_object(value, '$.groups[0].attrsList')) as value FROM tgt")
////    df2.printSchema()
////    df2.show(10)
////    df2.registerTempTable("df2")
////
////    val indexOfStringArray = (sa: String, )
//
//
//    // 1. read from json string to extracted json row
//    val readSql = "SELECT value FROM tgt"
//    val df = sqlContext.sql(readSql)
//    val rdd = df.map { row =>
//      row.getAs[String]("value")
//    }
//    val df1 = sqlContext.read.json(rdd)
//    df1.printSchema
//    df1.show(10)
//    df1.registerTempTable("df1")
//
//
//    val df2 = sqlContext.sql("select groups[0].attrsList as attrs from df1")
//    df2.printSchema
//    df2.show(10)
//    df2.registerTempTable("df2")
//    println(df2.count)
//
//    val indexOf = (arr: Seq[String], v: String) => {
//      arr.indexOf(v)
//    }
//    sqlContext.udf.register("index_of", indexOf)
//
//    val df3 = sqlContext.sql("select attrs.values[index_of(attrs.name, 'URL')][0] as url, cast(get_json_object(attrs.values[index_of(attrs.name, 'CRAWLMETADATA')][0], '$.tracker.crawlRequestCreateTS') as bigint) as ts from df2")
//    df3.printSchema()
//    df3.show(10)
//    df3.registerTempTable("df3")
//  }
//
//  test ("testing") {
//    val dt =
//      """
//        |{"name": "age", "age": 12, "items": [1, 2, 3],
//        |"subs": [{"id": 1, "type": "seed"}, {"id": 2, "type": "frog"}],
//        |"inner": {"a": 1, "b": 2}, "jstr": "{\"s1\": \"aaa\", \"s2\": 123}", "b": true
//        |}""".stripMargin
//    val rdd = sparkContext.parallelize(Seq(dt)).map(Row(_))
//    val vtp = StructField("value", StringType)
//    val df = sqlContext.createDataFrame(rdd, StructType(Array(vtp)))
//    df.registerTempTable("df")
//
//    val df1 = sqlContext.read.json(sqlContext.sql("select * from df").map(r => r.getAs[String]("value")))
//    df1.printSchema()
//    df1.show(10)
//    df1.registerTempTable("df1")
//
//    val test = (s: String) => {
//      s.toInt
//    }
//    sqlContext.udf.register("to_int", test)
//
//    val df2 = sqlContext.sql("select (b) as aa, inner.a from df1 where age = to_int(\"12\")")
//    df2.printSchema()
//    df2.show(10)
//  }
//
//  test ("test input only sql") {
//    val rdd0 = sparkContext.textFile("src/test/resources/input.msg").map(Row(_))
//
//    val vtp = StructField("value", StringType)
//    val df0 = sqlContext.createDataFrame(rdd0, StructType(Array(vtp)))
//    df0.registerTempTable("src")
//    df0.show(10)
//
//    // 1. read from json string to extracted json row
//    val df1 = sqlContext.sql("SELECT get_json_object(value, '$.seeds') as seeds FROM src")
//    df1.printSchema
//    df1.show(10)
//    df1.registerTempTable("df1")
//
//    val json2StringArray: (String) => Seq[String] = (s: String) => {
//      val seq = JsonUtil.fromJson[Seq[String]](s)
////      seq.map(i => JsonUtil.toJson(i))
//      seq
//    }
//    sqlContext.udf.register("json_to_string_array", json2StringArray)
//
//    val df2 = sqlContext.sql("SELECT explode(json_to_string_array(seeds)) as seed FROM df1")
//    df2.printSchema
//    df2.show(10)
//    df2.registerTempTable("df2")
//
//
//    val df3 = sqlContext.sql("SELECT get_json_object(seed, '$.url') as url, cast(get_json_object(get_json_object(seed, '$.metadata'), '$.tracker.crawlRequestCreateTS') as bigint) as ts FROM df2")
//    df3.printSchema
//    df3.show(10)
//  }
//
//  test ("test output only sql") {
//    val rdd0 = sparkContext.textFile("src/test/resources/output.msg").map(Row(_))
//
//    val vtp = StructField("value", StringType)
//    val df0 = sqlContext.createDataFrame(rdd0, StructType(Array(vtp)))
//    df0.registerTempTable("tgt")
//    df0.printSchema()
//    df0.show(10)
//
//    val json2StringArray: (String) => Seq[String] = (s: String) => {
//      JsonUtil.fromJson[Seq[String]](s)
//    }
//    sqlContext.udf.register("json_to_string_array", json2StringArray)
//
//    val json2StringJsonArray: (String) => Seq[String] = (s: String) => {
//      val seq = JsonUtil.fromJson[Seq[Any]](s)
//      seq.map(i => JsonUtil.toJson(i))
//    }
//    sqlContext.udf.register("json_to_string_json_array", json2StringJsonArray)
//
//    val indexOf = (arr: Seq[String], v: String) => {
//      arr.indexOf(v)
//    }
//    sqlContext.udf.register("index_of", indexOf)
//
//    val indexOfField = (arr: Seq[String], k: String, v: String) => {
//      val seq = arr.flatMap { item =>
//        JsonUtil.fromJson[Map[String, Any]](item).get(k)
//      }
//      seq.indexOf(v)
//    }
//    sqlContext.udf.register("index_of_field", indexOfField)
//
//    // 1. read from json string to extracted json row
//    val df1 = sqlContext.sql("SELECT get_json_object(value, '$.groups[0].attrsList') as attrs FROM tgt")
//    df1.printSchema
//    df1.show(10)
//    df1.registerTempTable("df1")
//
//    val df2 = sqlContext.sql("SELECT json_to_string_json_array(attrs) as attrs FROM df1")
//    df2.printSchema()
//    df2.show(10)
//    df2.registerTempTable("df2")
//
//    val df3 = sqlContext.sql("SELECT attrs[index_of_field(attrs, 'name', 'URL')] as attr1, attrs[index_of_field(attrs, 'name', 'CRAWLMETADATA')] as attr2 FROM df2")
//    df3.printSchema()
//    df3.show(10)
//    df3.registerTempTable("df3")
//
//    val df4 = sqlContext.sql("SELECT json_to_string_array(get_json_object(attr1, '$.values'))[0], cast(get_json_object(json_to_string_array(get_json_object(attr2, '$.values'))[0], '$.tracker.crawlRequestCreateTS') as bigint) FROM df3")
//    df4.printSchema()
//    df4.show(10)
//  }
//
//  test ("test from json") {
//    val fromJson2Map = (str: String) => {
//      val a = JsonUtil.fromJson[Map[String, Any]](str)
//      a.mapValues { v =>
//        v match {
//          case t: String => t
//          case _ => JsonUtil.toJson(v)
//        }
//      }
//    }
//    sqlContext.udf.register("from_json_to_map", fromJson2Map)
//
//    val fromJson2Array = (str: String) => {
//      val a = JsonUtil.fromJson[Seq[Any]](str)
//      a.map { v =>
//        v match {
//          case t: String => t
//          case _ => JsonUtil.toJson(v)
//        }
//      }
//    }
//    sqlContext.udf.register("from_json_to_array", fromJson2Array)
//
//    // ========================
//
//    val srdd = sparkContext.textFile("src/test/resources/input.msg").map(Row(_))
//    val svtp = StructField("value", StringType)
//    val sdf0 = sqlContext.createDataFrame(srdd, StructType(Array(svtp)))
//    sdf0.registerTempTable("sdf0")
//    sdf0.show(10)
//
//    // 1. read from json string to extracted json row
//    val sdf1 = sqlContext.sql("SELECT explode(from_json_to_array(get_json_object(value, '$.seeds'))) as seed FROM sdf0")
//    sdf1.printSchema
//    sdf1.show(10)
//    sdf1.registerTempTable("sdf1")
//
//    val sdf2 = sqlContext.sql("SELECT get_json_object(seed, '$.url') as url, cast(get_json_object(get_json_object(seed, '$.metadata'), '$.tracker.crawlRequestCreateTS') as bigint) as ts FROM sdf1")
//    sdf2.printSchema
//    sdf2.show(10)
//
//    // ---------------------------------------
//
//    val trdd = sparkContext.textFile("src/test/resources/output.msg").map(Row(_))
//    val tvtp = StructField("value", StringType)
//    val tdf0 = sqlContext.createDataFrame(trdd, StructType(Array(tvtp)))
//    tdf0.registerTempTable("tdf0")
//    tdf0.printSchema()
//    tdf0.show(10)
//
////    val json2StringArray: (String) => Seq[String] = (s: String) => {
////      JsonUtil.fromJson[Seq[String]](s)
////    }
////    sqlContext.udf.register("json_to_string_array", json2StringArray)
////
////    val json2StringJsonArray: (String) => Seq[String] = (s: String) => {
////      val seq = JsonUtil.fromJson[Seq[Any]](s)
////      seq.map(i => JsonUtil.toJson(i))
////    }
////    sqlContext.udf.register("json_to_string_json_array", json2StringJsonArray)
////
////    val indexOf = (arr: Seq[String], v: String) => {
////      arr.indexOf(v)
////    }
////    sqlContext.udf.register("index_of", indexOf)
////
//    val indexOfField = (arr: Seq[String], k: String, v: String) => {
//      val seq = arr.flatMap { item =>
//        JsonUtil.fromJson[Map[String, Any]](item).get(k)
//      }
//      seq.indexOf(v)
//    }
//    sqlContext.udf.register("index_of_field", indexOfField)
//
//    // 1. read from json string to extracted json row
////    val df1 = sqlContext.sql("SELECT get_json_object(value, '$.groups[0].attrsList') as attrs FROM tdf0")
//    val tdf1 = sqlContext.sql("SELECT from_json_to_array(get_json_object(value, '$.groups[0].attrsList')) as attrs FROM tdf0")
//    tdf1.printSchema
//    tdf1.show(10)
//    tdf1.registerTempTable("tdf1")
//
////    val tdf2 = sqlContext.sql("SELECT attrs[index_of_field(attrs, 'name', 'URL')] as attr1, attrs[index_of_field(attrs, 'name', 'CRAWLMETADATA')] as attr2 FROM tdf1")
////    tdf2.printSchema()
////    tdf2.show(10)
////    tdf2.registerTempTable("tdf2")
//
//    val tdf3 = sqlContext.sql("SELECT from_json_to_array(get_json_object(attrs[index_of_field(attrs, 'name', 'URL')], '$.values'))[0] as url, cast(get_json_object(from_json_to_array(get_json_object(attrs[index_of_field(attrs, 'name', 'CRAWLMETADATA')], '$.values'))[0], '$.tracker.crawlRequestCreateTS') as bigint) as ts FROM tdf1")
//    tdf3.printSchema()
//    tdf3.show(10)
//  }
//
//  test ("sql functions") {
//    val functions = sqlContext.sql("show functions")
//    functions.printSchema()
//    functions.show(10)
//
//    val functionNames = functions.map(_.getString(0)).collect
//    functionNames.foreach(println)
//  }
//
//  test ("test text file read") {
//    val partitionPaths = Seq[String](
//      "hdfs://localhost/griffin/streaming/dump/source/418010/25080625/1504837518000",
//      "hdfs://localhost/griffin/streaming/dump/target/418010/25080625/1504837518000")
//    val df = sqlContext.read.json(partitionPaths: _*)
//    df.printSchema()
//    df.show(10)
//  }
//
//  test ("list paths") {
//    val filePath = "hdfs://localhost/griffin/streaming/dump/source"
//    val partitionRanges = List[(Long, Long)]((0, 0), (-2, 0))
//    val partitionPaths = listPathsBetweenRanges(filePath :: Nil, partitionRanges)
//    println(partitionPaths)
//  }
//
//  private def listPathsBetweenRanges(paths: List[String],
//                                     partitionRanges: List[(Long, Long)]
//                                    ): List[String] = {
//    partitionRanges match {
//      case Nil => paths
//      case head :: tail => {
//        val (lb, ub) = head
//        val curPaths = paths.flatMap { path =>
//          val names = HdfsUtil.listSubPathsByType(path, "dir").toList
//          println(names)
//          names.filter { name =>
//            str2Long(name) match {
//              case Some(t) => (t >= lb) && (t <= ub)
//              case _ => false
//            }
//          }.map(HdfsUtil.getHdfsFilePath(path, _))
//        }
//        listPathsBetweenRanges(curPaths, tail)
//      }
//    }
//  }
//
//  private def str2Long(str: String): Option[Long] = {
//    try {
//      Some(str.toLong)
//    } catch {
//      case e: Throwable => None
//    }
//  }
//}