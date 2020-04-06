package org.apache.griffin.measure.datasource.connector.batch

import scala.io.Source

import org.apache.spark.sql.types.StructType
import org.scalatest.Matchers
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, IndexSettings, PopularProperties}

import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.datasource.TimestampStorage

class ElasticSearchDataConnectorTest extends SparkSuiteBase with Matchers {

  private final val ES_VERSION = "6.8.8"
  private final val ES_CLUSTER_NAME = "mock_cluster"
  private final val ES_INTERNAL_PORT = 1506
  private final val ES_HTTP_PORT = 1507
  private final val INDEX1 = "bank"
  private final val INDEX2 = "car"

  private final val timestampStorage = TimestampStorage()

  private final val dcParam =
    DataConnectorParam(
      conType = "es_dcp",
      dataFrameName = "test_df",
      config = Map.empty,
      preProc = Nil)

  private var embeddedServer: EmbeddedElastic = _

  private def getIndexSettings(indexName: String, indexTemplatePath: String) =
    IndexSettings
      .builder()
      .withType(indexName, ClassLoader.getSystemResourceAsStream(indexTemplatePath))
      .build()

  override def beforeAll(): Unit = {
    super.beforeAll()

    embeddedServer = EmbeddedElastic
      .builder()
      .withElasticVersion(ES_VERSION)
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, ES_INTERNAL_PORT)
      .withSetting(PopularProperties.HTTP_PORT, ES_HTTP_PORT)
      .withSetting(PopularProperties.CLUSTER_NAME, ES_CLUSTER_NAME)
      .withIndex(INDEX1, getIndexSettings(INDEX1, "elasticsearch/test_data_1_template.json"))
      .withIndex(INDEX2, getIndexSettings(INDEX2, "elasticsearch/test_data_2_template.json"))
      .build()
      .start()

    val dataSrc1 =
      Source.fromFile(ClassLoader.getSystemResource("elasticsearch/test_data_1.json").toURI)
    embeddedServer.index(INDEX1, INDEX1, dataSrc1.getLines().toList: _*)

    dataSrc1.close()

    val dataSrc2 =
      Source.fromFile(ClassLoader.getSystemResource("elasticsearch/test_data_2.json").toURI)
    embeddedServer.index(INDEX2, INDEX2, dataSrc2.getLines().toList: _*)

    dataSrc2.close()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    embeddedServer.deleteIndices()
    embeddedServer.deleteTemplates()
    embeddedServer.stop()
  }

//  private def generateData

  "elastic search data connector" should "be able to read from embedded server" in {
    val configs = Map(
      "paths" -> Seq(INDEX1),
      "options" -> Map("es.nodes" -> "localhost", "es.port" -> ES_HTTP_PORT))
    val dc = ElasticSearchDataConnector(spark, dcParam.copy(config = configs), timestampStorage)
    val result = dc.data(1000L)

    assert(result._1.isDefined)
    assert(result._1.get.collect().length == 999)
  }

  it should "be able to read from multiple indices and merge their schemas" in {
    val configs = Map(
      "paths" -> Seq(INDEX1, INDEX2),
      "options" -> Map("es.nodes" -> "localhost", "es.port" -> ES_HTTP_PORT))
    val dc = ElasticSearchDataConnector(spark, dcParam.copy(config = configs), timestampStorage)
    val result = dc.data(1000L)

    assert(result._1.isDefined)
    assert(result._1.get.collect().length == 1001)

    val expectedSchema = new StructType()
      .add("description", "string")
      .add("manufacturer", "string")
      .add("model", "string")
      .add("account_number", "bigint")
      .add("address", "string")
      .add("age", "bigint")
      .add("balance", "bigint")
      .add("city", "string")
      .add("email", "string")
      .add("employer", "string")
      .add("firstname", "string")
      .add("gender", "string")
      .add("lastname", "string")
      .add("state", "string")
      .add("__tmst", "bigint", nullable = false)

    result._1.get.schema.fields should contain theSameElementsAs expectedSchema.fields
  }

  it should "respect selection expression" in {
    val configs = Map(
      "paths" -> Seq(INDEX1, INDEX2),
      "options" -> Map("es.nodes" -> "localhost", "es.port" -> ES_HTTP_PORT),
      "selectionExprs" -> Seq("account_number", "age > 10 as is_adult"))
    val dc = ElasticSearchDataConnector(spark, dcParam.copy(config = configs), timestampStorage)
    val result = dc.data(1000L)

    assert(result._1.isDefined)
    assert(result._1.get.collect().length == 1001)

    val expectedSchema = new StructType()
      .add("account_number", "bigint")
      .add("is_adult", "boolean")
      .add("__tmst", "bigint", nullable = false)

    result._1.get.schema.fields should contain theSameElementsAs expectedSchema.fields
  }

  it should "respect filter conditions" in {
    val configs = Map(
      "paths" -> Seq(INDEX1, INDEX2),
      "options" -> Map("es.nodes" -> "localhost", "es.port" -> ES_HTTP_PORT),
      "selectionExprs" -> Seq("account_number"),
      "filterExprs" -> Seq("account_number < 10"))
    val dc = ElasticSearchDataConnector(spark, dcParam.copy(config = configs), timestampStorage)
    val result = dc.data(1000L)

    assert(result._1.isDefined)
    assert(result._1.get.collect().length == 10)

    val expectedSchema = new StructType()
      .add("account_number", "bigint")
      .add("__tmst", "bigint", nullable = false)

    result._1.get.schema.fields should contain theSameElementsAs expectedSchema.fields
  }

}
