package modelTest

import org.apache.griffin.accuracy._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.io.{FileInputStream, FileOutputStream}

import org.apache.griffin.dataLoaderUtils.{DataLoaderFactory, FileLoaderUtil}

import scala.collection.mutable.MutableList
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class AccuTest extends FunSuite with Matchers with BeforeAndAfter {

  val dataFilePath = FileLoaderUtil.convertPath("data/test/dataFile/")
  val reqJsonPath = FileLoaderUtil.convertPath("data/test/reqJson/")
  val recordFilePath = FileLoaderUtil.convertPath("data/test/recordFile/")
  val recordFileName = "_RESULT_ACCU"

  case class AccuData() {
    var cnt: Int = _
    var reqJson: String = _
    var configure: AccuracyConfEntity = _
    var dataFrameSrc: DataFrame = _
    var dataFrameTgt: DataFrame = _
    var result: ((Long, Long), List[String]) = _
  }
  val accuDatas = MutableList[AccuData]()

  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AccTest")
    sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    var cnt = 1;
    val accTests = List("accuAvroTest.json", "accuUsersInfo.json")
    for (tf <- accTests) {
      val reqJson = reqJsonPath + tf
      val accuData = new AccuData()
      accuData.cnt = cnt
      accuData.reqJson = reqJson
      val input = new FileInputStream(reqJson)
      accuData.configure = mapper.readValue(input, classOf[AccuracyConfEntity])
      val dataLoader = DataLoaderFactory.getDataLoader(sqlContext, DataLoaderFactory.avro, dataFilePath)
      val dfs = dataLoader.getAccuDataFrame(accuData.configure)
      accuData.dataFrameSrc = dfs._1
      accuData.dataFrameTgt = dfs._2
      accuDatas += accuData
      cnt += 1
    }
  }

  test("test accuracy requests") {
    for (accuData <- accuDatas) {
      //-- algorithm --
      accuData.result = Accu.calcAccu(accuData.configure, accuData.dataFrameSrc, accuData.dataFrameTgt)
    }
  }

  after {
    val out = new FileOutputStream(recordFilePath + recordFileName)
    for (accuData <- accuDatas) {
      //output
      out.write(("//" + "=" * 10).getBytes("utf-8"))
      out.write((s" ${accuData.cnt}. Test Accuracy model result with request file: ${accuData.reqJson} ").getBytes("utf-8"))
      out.write(("=" * 10 + "\n").getBytes("utf-8"))

      val ((missCount, srcCount), missedList) = accuData.result
      val rslt = s"match percentage: ${((1 - missCount.toDouble / srcCount) * 100)} %"
      val rcds = missedList.mkString("\n")
      val rcd = rslt + "\n\n" + rcds + "\n\n";

      out.write(rcd.getBytes("utf-8"))
    }
    out.close()
    sc.stop()
  }
}
