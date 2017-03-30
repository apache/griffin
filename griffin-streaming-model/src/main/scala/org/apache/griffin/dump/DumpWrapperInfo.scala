package org.apache.griffin.dump

sealed trait DumpWrapperInfo {
  type T
  val key: String
  val tp: String
  def wrap(value: T) = (key -> value)
}

final case object TimeGroupInfo extends DumpWrapperInfo {
  type T = Long
  val key = "__time__"
  val tp = "bigint"
}

final case object NextFireTimeInfo extends DumpWrapperInfo {
  type T = Long
  val key = "__next_fire_time__"
  val tp = "bigint"
}

final case object MismatchInfo extends DumpWrapperInfo {
  type T = String
  val key = "__mismatch__"
  val tp = "string"
}

final case object ErrorInfo extends DumpWrapperInfo {
  type T = String
  val key = "__error__"
  val tp = "string"
}
