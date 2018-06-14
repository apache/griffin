package org.apache.griffin.measure.configuration.params.reader

import org.apache.griffin.measure.configuration.params.DQConfig
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.util.{Failure, Success}

class ParamJsonReaderSpec extends FlatSpec with Matchers{


  "params " should "be parsed from a valid file" in {
    val bufferedSource = Source.fromFile(getClass.getResource("/_accuracy-batch-sparksql.json").getFile)
    val jsonString = bufferedSource.getLines().mkString
    bufferedSource.close

    val reader :ParamReader = ParamJsonReader(jsonString)
    val params = reader.readConfig[DQConfig]
    params match {
      case Success(v) =>
        v.evaluateRule.getRules(0).dslType should === ("spark-sql")
        v.evaluateRule.getRules(0).name should === ("missRecords")
      case Failure(_) =>
        fail("it should not happen")
    }

  }

  it should "fail for an invalid file" in {
    val bufferedSource = Source.fromFile(getClass.getResource("/invalidconfigs/missingrule_accuracy_batch_sparksql.json").getFile)
    val jsonString = bufferedSource.getLines().mkString
    bufferedSource.close

    val reader :ParamReader = ParamJsonReader(jsonString)
    val params = reader.readConfig[DQConfig]
    params match {
      case Success(_) =>
        fail("it is an invalid config file")
      case Failure(e) =>
        e.getMessage should include ("evaluate.rule should not be null")
    }

  }

}


