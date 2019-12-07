package org.apache.griffin.measure.configuration.dqdefinition.reader
import org.apache.griffin.measure.configuration.dqdefinition.{DQConfig, EvaluateRuleParam, RuleOutputParam, RuleParam}
import org.apache.griffin.measure.configuration.enums.{DqType, DslType, FlattenType, OutputType, SinkType}
import org.scalatest.{FlatSpec, Matchers}

class ParamEnumReaderSpec extends FlatSpec with Matchers {
  "dsltype" should "be parsed to predefined set of values" in {
    val validDslSparkSqlValues =
      Seq("spark-sql", "spark-SQL", "SPARK-SQL", "sparksql")
    validDslSparkSqlValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should ===(DslType.SparkSql)
    }
    val invalidDslSparkSqlValues = Seq("spark", "sql", "")
    invalidDslSparkSqlValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should not be (DslType.SparkSql)
    }

    val validDslGriffinValues =
      Seq("griffin-dsl", "griffindsl", "griFfin-dsl", "")
    validDslGriffinValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should ===(DslType.GriffinDsl)
    }

    val validDslDfOpsValues = Seq(
      "df-ops",
      "dfops",
      "DFOPS",
      "df-opr",
      "dfopr",
      "df-operations",
      "dfoperations"
    )
    validDslDfOpsValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should ===(DslType.DfOperations)
    }

    val invalidDslDfOpsValues = Seq("df-oprts", "-")
    invalidDslDfOpsValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should not be (DslType.DfOperations)
    }
    /*val idPattern = "^(?i)spark-?sql$".r
    idPattern.findFirstMatchIn("spark-SQL") match {
      case Some(_) => println("ok")
      case None  => println("not ok")
    }*/
  }

  "griffindsl" should "be returned as default dsl type" in {
    val dslGriffinDslValues = Seq("griffin", "dsl")
    dslGriffinDslValues foreach { x =>
      val ruleParam = new RuleParam(x, "accuracy")
      ruleParam.getDslType should be(DslType.GriffinDsl)
    }
    /*val idPattern = "^(?i)spark-?sql$".r
    idPattern.findFirstMatchIn("spark-SQL") match {
      case Some(_) => println("ok")
      case None  => println("not ok")
    }*/
  }

  "dqtype" should "be parsed to predefined set of values" in {
    var ruleParam = new RuleParam("griffin-dsl", "accuracy")
    ruleParam.getDqType should be(DqType.Accuracy)
    ruleParam = new RuleParam("griffin-dsl", "accu")
    ruleParam.getDqType should not be (DqType.Accuracy)

    ruleParam = new RuleParam("griffin-dsl", "profiling")
    ruleParam.getDqType should be(DqType.Profiling)
    ruleParam = new RuleParam("griffin-dsl", "profilin")
    ruleParam.getDqType should not be (DqType.Profiling)

    ruleParam = new RuleParam("griffin-dsl", "TIMELINESS")
    ruleParam.getDqType should be(DqType.Timeliness)
    ruleParam = new RuleParam("griffin-dsl", "timeliness ")
    ruleParam.getDqType should not be (DqType.Timeliness)

    ruleParam = new RuleParam("griffin-dsl", "UNIQUENESS")
    ruleParam.getDqType should be(DqType.Uniqueness)
    ruleParam = new RuleParam("griffin-dsl", "UNIQUE")
    ruleParam.getDqType should not be (DqType.Uniqueness)

    ruleParam = new RuleParam("griffin-dsl", "Duplicate")
    ruleParam.getDqType should be(DqType.Uniqueness)
    ruleParam = new RuleParam("griffin-dsl", "duplica")
    ruleParam.getDqType should not be (DqType.Duplicate)

    ruleParam = new RuleParam("griffin-dsl", "COMPLETENESS")
    ruleParam.getDqType should be(DqType.Completeness)
    ruleParam = new RuleParam("griffin-dsl", "complete")
    ruleParam.getDqType should not be (DqType.Completeness)

    ruleParam = new RuleParam("griffin-dsl", "")
    ruleParam.getDqType should be(DqType.Unknown)
    ruleParam = new RuleParam("griffin-dsl", "duplicate")
    ruleParam.getDqType should not be (DqType.Unknown)
  }

  "outputtype" should "be valid" in {
    var ruleOutputParam = new RuleOutputParam("metric","","map")
    ruleOutputParam.getOutputType should be(OutputType.MetricOutputType)
    ruleOutputParam = new RuleOutputParam("metr","","map")
    ruleOutputParam.getOutputType should not be (OutputType.MetricOutputType)
    ruleOutputParam.getOutputType should be (OutputType.UnknownOutputType)

    ruleOutputParam = new RuleOutputParam("record","","map")
    ruleOutputParam.getOutputType should be(OutputType.RecordOutputType)
    ruleOutputParam = new RuleOutputParam("rec","","map")
    ruleOutputParam.getOutputType should not be (OutputType.RecordOutputType)
    ruleOutputParam.getOutputType should be (OutputType.UnknownOutputType)

    ruleOutputParam = new RuleOutputParam("dscupdate","","map")
    ruleOutputParam.getOutputType should be(OutputType.DscUpdateOutputType)
    ruleOutputParam = new RuleOutputParam("dsc","","map")
    ruleOutputParam.getOutputType should not be (OutputType.DscUpdateOutputType)
    ruleOutputParam.getOutputType should be (OutputType.UnknownOutputType)

  }

  "flattentype" should "be valid" in {
    var ruleOutputParam = new RuleOutputParam("metric","","map")
    ruleOutputParam.getFlatten should be(FlattenType.MapFlattenType)
    ruleOutputParam = new RuleOutputParam("metric","","metr")
    ruleOutputParam.getFlatten should not be (FlattenType.MapFlattenType)
    ruleOutputParam.getFlatten should be (FlattenType.DefaultFlattenType)

    ruleOutputParam = new RuleOutputParam("metric","","array")
    ruleOutputParam.getFlatten should be(FlattenType.ArrayFlattenType)
    ruleOutputParam = new RuleOutputParam("metric","","list")
    ruleOutputParam.getFlatten should be(FlattenType.ArrayFlattenType)
    ruleOutputParam = new RuleOutputParam("metric","","arrays")
    ruleOutputParam.getFlatten should not be (FlattenType.ArrayFlattenType)
    ruleOutputParam.getFlatten should be (FlattenType.DefaultFlattenType)

    ruleOutputParam = new RuleOutputParam("metric","","entries")
    ruleOutputParam.getFlatten should be(FlattenType.EntriesFlattenType)
    ruleOutputParam = new RuleOutputParam("metric","","entry")
    ruleOutputParam.getFlatten should not be (FlattenType.EntriesFlattenType)
    ruleOutputParam.getFlatten should be (FlattenType.DefaultFlattenType)
  }

  "sinktype" should "be valid" in {
    import org.mockito.Mockito._
    var dqConfig = new DQConfig("test",1234,"",Nil,mock(classOf[EvaluateRuleParam]),List("Console","Log","CONSOLE","LOG","Es","ElasticSearch","Http","MongoDB","mongo","hdfs"))
    dqConfig.getValidSinkTypes should be (Seq(SinkType.Console,SinkType.ElasticSearch,SinkType.MongoDB,SinkType.Hdfs))
    dqConfig = new DQConfig("test",1234,"",Nil,mock(classOf[EvaluateRuleParam]),List("Consol","Logg"))
    dqConfig.getValidSinkTypes should not be (Seq(SinkType.Console))
    dqConfig.getValidSinkTypes should be (Seq(SinkType.ElasticSearch))


    dqConfig = new DQConfig("test",1234,"",Nil,mock(classOf[EvaluateRuleParam]),List(""))
    dqConfig.getValidSinkTypes should be (Seq(SinkType.ElasticSearch))
  }

}
