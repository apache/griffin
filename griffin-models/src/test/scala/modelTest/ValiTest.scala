package modelTest

import org.apache.griffin._
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
class ValiTest extends FunSuite with Matchers with BeforeAndAfter {

  val dataFilePath = FileLoaderUtil.convertPath("data/test/dataFile/")
  val reqJsonPath = FileLoaderUtil.convertPath("data/test/reqJson/")
  val recordFilePath = FileLoaderUtil.convertPath("data/test/recordFile/")
  val recordFileName = "_RESULT_VALI"

  case class ValiData() {
    var cnt: Int = _
    var reqJson: String = _
    var configure: ValidityConfEntity = _
    var dataFrame: DataFrame = _
  }
  val valiDatas = MutableList[ValiData]()

  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AccTest")
    sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    var cnt = 1;
    val valiTests = List("valiAvroTest.json")
    for (tf <- valiTests) {
      val reqJson = reqJsonPath + tf
      val valiData = new ValiData()
      valiData.cnt = cnt
      valiData.reqJson = reqJson
      val input = new FileInputStream(reqJson)
      valiData.configure = mapper.readValue(input, classOf[ValidityConfEntity])
      val dataLoader = DataLoaderFactory.getDataLoader(sqlContext, DataLoaderFactory.avro, dataFilePath)
      valiData.dataFrame = dataLoader.getValiDataFrame(valiData.configure)
      valiDatas += valiData
      cnt += 1
    }
  }

  test("test validity requests") {
    for (valiData <- valiDatas) {
      //-- algorithm --
      Vali.calcVali(valiData.configure, valiData.dataFrame)
    }
  }

  after {
    val out = new FileOutputStream(recordFilePath + recordFileName)
    for (valiData <- valiDatas) {
      //output
      out.write(("//" + "=" * 10).getBytes("utf-8"))
      out.write((s" ${valiData.cnt}. Test Validity model result with request file: ${valiData.reqJson} ").getBytes("utf-8"))
      out.write(("=" * 10 + "\n").getBytes("utf-8"))

      val rcd = valiData.configure.toString + "\n\n"
      out.write(rcd.getBytes("utf-8"))
    }
    out.close()
    sc.stop()
  }
}
