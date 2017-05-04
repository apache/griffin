package org.apache.griffin.measure.batch.result


sealed trait ResultInfo {
  type T
  val key: String
  val tp: String
  def wrap(value: T) = (key -> value)
}

final case object TimeGroupInfo extends ResultInfo {
  type T = Long
  val key = "__time__"
  val tp = "bigint"
}

final case object NextFireTimeInfo extends ResultInfo {
  type T = Long
  val key = "__next_fire_time__"
  val tp = "bigint"
}

final case object MismatchInfo extends ResultInfo {
  type T = String
  val key = "__mismatch__"
  val tp = "string"
}

final case object TargetInfo extends ResultInfo {
  type T = Map[String, Any]
  val key = "__target__"
  val tp = "map"
}

final case object ErrorInfo extends ResultInfo {
  type T = String
  val key = "__error__"
  val tp = "string"
}
