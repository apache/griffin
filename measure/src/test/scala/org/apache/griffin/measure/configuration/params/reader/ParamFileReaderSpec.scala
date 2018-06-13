package org.apache.griffin.measure.configuration.params.reader
import org.apache.griffin.measure.configuration.params.DQConfig
import org.scalatest._


import scala.util.{Failure, Success}

class ParamFileReaderSpec extends FlatSpec with Matchers{


  "params " should "be parsed from a valid file" in {
    val reader :ParamReader = ParamFileReader(getClass.getResource("/_accuracy-batch-sparksql.json").getFile)
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
    val reader :ParamReader = ParamFileReader(getClass.getResource("/invalidconfigs/missingrule_accuracy_batch_sparksql.json").getFile)
    val params = reader.readConfig[DQConfig]
    params match {
      case Success(_) =>
        fail("it is an invalid config file")
      case Failure(e) =>
        e.getMessage contains ("evaluate.rule should not be null")
    }

  }

}
