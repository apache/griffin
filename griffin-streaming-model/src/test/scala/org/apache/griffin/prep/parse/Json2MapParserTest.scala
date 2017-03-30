package org.apache.griffin.prep.parse

import org.apache.griffin.config.{ConfigFileReader, ConfigReader}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.apache.griffin.config.params._
import org.apache.griffin.utils.{ExtractJsonUtil, JsonUtil}

import scala.io.Source
import scala.util.{Failure, Try}


@RunWith(classOf[JUnitRunner])
class Json2MapParserTest extends FunSuite with Matchers with BeforeAndAfter {

//  test("test json 2 map parser") {
//
//    val reader: ConfigReader = ConfigFileReader("src/main/resources/config.json")
//    val param = reader.readConfig[AllParam]
//
//    val lines = Source.fromFile("src/test/resources/input.msg").getLines
//
//    param.dataAssetParamMap.get("source") match {
//      case Some(dataAssetParam: DataAssetParam) => {
//        val configParam = dataAssetParam.prepParam.parseParam.configParam
//        println(configParam)
//        val parser = Json2MapParser(configParam)
//
//        lines.foreach { line =>
//          val res = parser.parse(line)
//
//          val record: String = res.map { r =>
//            r.values.mkString(",")
//          }.mkString("\n")
//
//          println(record)
//
////          for (mp <- res) {
////
////            println(mp)
////          }
//        }
//      }
//    }
//
//  }

}
