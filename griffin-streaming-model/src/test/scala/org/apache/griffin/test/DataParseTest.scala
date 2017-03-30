package org.apache.griffin.test

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.griffin.config.params._
import org.apache.griffin.utils.{ExtractJsonUtil, JsonUtil}

import scala.io.Source


@RunWith(classOf[JUnitRunner])
class DataParseTest extends FunSuite with Matchers with BeforeAndAfter {

//  test ("test extract list") {
//    val urlSteps = List("json", ".seeds", "[*]", "json", ".url")
//    val createdTsSteps = List("json", ".seeds", "[*]", "json", ".metadata", "json", ".tracker", ".crawlRequestCreateTS")
//
//    val lines = Source.fromFile("src/test/resources/input.msg").getLines
//
//    lines.foreach { line =>
//
//      val datas = Some(line) :: Nil
//      val schema = Map(("url" -> urlSteps), ("createdTs" -> createdTsSteps))
//      val result = ExtractJsonUtil.extractDataListWithSchemaMap(datas, schema)
//
//      result.foreach { res =>
//        val r = res.mapValues(_.getOrElse("none"))
//        println(r)
//      }
//
//    }
//  }

  test ("test extract list 1111") {
    val urlSteps = List("json", ".seeds", "[*]", "json", ".url")
    val createdTsSteps = List("json", ".seeds", "[*]", "json", ".metadata", "json", ".tracker", ".crawlRequestCreateTS")

    val lines = Source.fromFile("src/test/resources/input.msg").getLines

    lines.foreach { line =>

      val datas = Some(line) :: Nil
      val schema = Map(("url" -> urlSteps), ("createdTs" -> createdTsSteps))
      val result = ExtractJsonUtil.extractDataListWithSchemaMap(datas, schema)

      result.foreach { res =>
        val r = res.mapValues(_.getOrElse("none"))
        println(r)
      }

    }
  }

  test ("test extract list 2222") {
    val urlSteps = List("json", ".groups", "[0]", ".attrsList", "[.name=URL]", ".values", "[0]")
    val createdTsSteps = List("json", ".groups", "[0]", ".attrsList", "[.name=CRAWLMETADATA]", ".values", "[0]", "json", ".tracker", ".crawlRequestCreateTS")

//    val lines = Source.fromFile("src/test/resources/out1.msg").getLines
    val lines = Source.fromFile("src/test/resources/output.msg").getLines

    lines.foreach { line =>

      val datas = Some(line) :: Nil
      val schema = Map(("url" -> urlSteps), ("createdTs" -> createdTsSteps))
      val result = ExtractJsonUtil.extractDataListWithSchemaMap(datas, schema)

      result.foreach { res =>
        val r = res.mapValues(_.getOrElse("none"))
        println(r)
      }

    }
  }

//  test ("test cret") {
//    val nameSteps = List("json", ".spark", ".app.name")
//    val logLevelSteps = List("json", ".spark", ".log.level")
//    val sourceOutSchemaNameSteps = List("json", ".dataAsset", ".source", ".pre.process", ".parse", ".config", ".out.schema", "[*]", ".name")
//    val sourceOutSchemaNameSteps1 = List("json", ".dataAsset", ".source", ".pre.process", ".parse", ".config", ".out.schema", "[0]", ".name")
//    val sourceOutSchemaNameSteps2 = List("json", ".dataAsset", ".source", ".pre.process", ".parse", ".config", ".out.schema", "[*]",
//      ".extract.steps", "[*]")
//    val sourceOutSchemaNameSteps3 = List("json", ".dataAsset", ".source", ".pre.process", ".parse", ".config", ".out.schema", "[*]",
//      ".extract.steps", "[4]")
//
//    val file = "src/main/resources/config.json"
//    val line = scala.io.Source.fromFile(file).mkString
//
//    val datas = Some(line) :: Nil
//    val schema = Map(
//      ("name" -> nameSteps),
//      ("loglevel" -> logLevelSteps),
//      ("srcschemaname" -> sourceOutSchemaNameSteps),
//      ("test1" -> sourceOutSchemaNameSteps1),
//      ("test2" -> sourceOutSchemaNameSteps2),
//      ("test3" -> sourceOutSchemaNameSteps3)
//    )
//    ExtractJsonUtil.extractDataListWithSchemaMapTest(datas, schema)
//  }

}
