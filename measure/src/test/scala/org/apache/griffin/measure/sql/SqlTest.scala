//package org.apache.griffin.measure.sql
//
//import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
//import org.apache.griffin.measure.rule.expr.{Expr, StatementExpr}
//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//@RunWith(classOf[JUnitRunner])
//class SqlTest extends FunSuite with BeforeAndAfter with Matchers {
//
//  var sc: SparkContext = _
//  var sqlContext: SQLContext = _
//
//  before {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
//    sc = new SparkContext(conf)
//    sqlContext = new SQLContext(sc)
//  }
//
//  test ("spark sql") {
//
//    val squared = (s: Int) => {
//      s * s
//    }
//    sqlContext.udf.register("square", squared)
//
//    val a = sqlContext.range(1, 20)
//    a.show
//
//    a.registerTempTable("test")
//
//    val table = sqlContext.sql("select * from test")
//    table.show()
//
//    val result = sqlContext.sql("select id, square(id) as id_squared from test")
//    result.show()
//
//  }
//
//  test ("json") {
//    def jsonToDataFrame(json: String, schema: Option[StructType] = None): DataFrame = {
//      val reader = sqlContext.read
//      val rd = schema match {
//        case Some(scm) => reader.schema(scm)
//        case _ => reader
//      }
//      rd.json(sc.parallelize(json :: Nil))
//    }
//
//    val json =
//      """
//        |{
//        |  "a": [
//        |     1, 2, 3
//        |  ]
//        |}
//      """.stripMargin
//
////    val bt = StructField("b", IntegerType)
////    val at = StructField("a", StructType(bt :: Nil))
////    val schema = StructType(at :: Nil)
//
//    val at = StructField("a", ArrayType(IntegerType))
//    val schema = StructType(at :: Nil)
//
//    val df = jsonToDataFrame(json, Some(schema))
//
//    df.registerTempTable("json")
//
//    val result = sqlContext.sql("select a[1] from json")
//    result.show
//
//  }
//
//  test ("json file") {
//
//    // read json file directly
////    val filePath = "src/test/resources/test-data.jsonFile"
////    val reader = sqlContext.read
////    val df = reader.json(filePath)
////    df.show
////
////    df.registerTempTable("ttt")
////    val result = sqlContext.sql("select * from ttt where list[0].c = 11")
////    result.show
//
//    // whole json file
////    val filePath = "src/test/resources/test-data0.json"
//////    val filePath = "hdfs://localhost/test/file/t1.json"
////    val jsonRDD = sc.wholeTextFiles(s"${filePath},${filePath}").map(x => x._2)
////    val namesJson = sqlContext.read.json(jsonRDD)
////    namesJson.printSchema
////    namesJson.show
//
//    // read text file then convert to json
//    val filePath = "src/test/resources/test-data.jsonFile"
//    val rdd = sc.textFile(filePath)
//    val reader = sqlContext.read
//    val df = reader.json(rdd)
//    df.show
//    df.printSchema
//
//    df.registerTempTable("ttt")
//    val result = sqlContext.sql("select * from ttt where list[0].c = 11")
//    result.show
//
//    // udf
//    val slice = (arr: Seq[Long], f: Int, e: Int) => arr.slice(f, e)
////    val slice = (arr: Seq[Long]) => arr.slice(0, 1)
//    sqlContext.udf.register("slice", slice)
//
//    val result1 = sqlContext.sql("select slice(t, 0, 2) from ttt")
//    result1.show
//
//  }
//
//  test ("accu sql") {
////    val file1 =
//  }
//
//}
