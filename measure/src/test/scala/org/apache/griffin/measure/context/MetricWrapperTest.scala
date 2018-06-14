package org.apache.griffin.measure.context

import org.scalatest._

class MetricWrapperTest extends FlatSpec with Matchers {

  "metric wrapper" should "flush empty if no metric inserted" in {
    val metricWrapper = MetricWrapper("name")
    metricWrapper.flush should be (Map[Long, Map[String, Any]]())
  }

  it should "flush all metrics inserted" in {
    val metricWrapper = MetricWrapper("test")
    metricWrapper.insertMetric(1, Map("total" -> 10, "miss"-> 2))
    metricWrapper.insertMetric(1, Map("match" -> 8))
    metricWrapper.insertMetric(2, Map("total" -> 20))
    metricWrapper.insertMetric(2, Map("miss" -> 4))
    metricWrapper.flush should be (Map(
      1L -> Map("name" -> "test", "tmst" -> 1, "value" -> Map("total" -> 10, "miss"-> 2, "match" -> 8)),
      2L -> Map("name" -> "test", "tmst" -> 2, "value" -> Map("total" -> 20, "miss"-> 4))
    ))
  }

}
