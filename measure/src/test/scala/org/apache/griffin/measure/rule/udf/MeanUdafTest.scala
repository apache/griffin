package org.apache.griffin.measure.rule.udf


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite


class MeanUdafTest extends FunSuite  {

  val sparkConf = new SparkConf()
  sparkConf.setMaster("local")
  //sparkConf.setSparkHome(System.getenv("SPARK_HOME"))
  sparkConf.setAppName("UDFTest")

  val sparkContext = new SparkContext(sparkConf)

  implicit val sqlContext = new SQLContext(sparkContext)

  import sqlContext.implicits._


  test("test the udf"){
    // the udf will not work with nulls.
    val data1 = Seq[(Integer, Integer)](
      (1, 10),
      (null, -60),
      (1, 20),
      (1, 30),
      (2, 0),
      (null, -10),
      (2, -1),
      (2, null),
      (2, null),
      (null, 100),
      (3, null),
      (null, null),
      (3, null)).toDF("key", "value")
    data1.registerTempTable("agg1")

    val data2 = Seq[(Integer, Integer)](
      (1, 10),
      (1, -60),
      (1, 20),
      (1, 30),
      (2, 0),
      (2, -10),
      (2, -1),
      (2, -5),
      (2, 5),
      (3, 100),
      (3, -10),
      (3, 10),
      (3, 10)).toDF("key", "value")
    data2.registerTempTable("agg2")
    //your unit test assert here like below
    assert("True".toLowerCase == "true")
    sqlContext.udf.register("my_mean", new MeanUdaf)

    val nullFreeDf = sqlContext.sql(

      """
        |SELECT
        |  key,
        |  my_mean(value) as udfVal,
        |  avg(value) as defaultVal,
        |  count(*) as totalCount,
        |  sum(value) as totalSum
        |FROM agg2
        |GROUP BY key
      """.stripMargin).toDF()


    nullFreeDf.registerTempTable("agg0")
    nullFreeDf.show()

    val result = sqlContext.sql(

      """
        |SELECT
        | SUM(IF(udfVal=defaultVal, 0, 1)) equalCols, SUM(IF(udfVal=totalSum/totalCount, 0, 1)) avgCols
        |FROM agg0
      """.stripMargin).toDF()

    result.show()

    assert(result.count() === 1)

    assert(result.head().getLong(0) === 0L)
    assert(result.head().getLong(1) === 0L)

  }



}
