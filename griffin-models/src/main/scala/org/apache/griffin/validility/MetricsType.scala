package org.apache.griffin.validility

object MetricsType extends Enumeration{
  type MetricsType = Value
  val DefaultCount = Value(0, "defaultCount")
  val TotalCount = Value(1, "totalCount")
  val NullCount = Value(2, "nullCount")
  val UniqueCount = Value(3, "uniqueCount")
  val DuplicateCount = Value(4, "duplicateCount")
  val Maximum = Value(5, "maximum")
  val Minimum = Value(6, "minimum")
  val Mean = Value(7, "mean")
  val Median = Value(8, "median")
  val RegularExp = Value(9, "regularExp")
  val PatternFreq = Value(10, "patternFreq")
}