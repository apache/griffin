package org.apache.griffin.accuracy

/**
  * mapping between source column and target column
  *
  */
class AccuracyMapping {
  var sourceColId: Int = _
  var sourceColName: String = _
  var sourceConvertingFunctions: List[String] = List()

  var targetColId: Int = _
  var targetColName: String = _
  var targetConvertingFunctions: List[String] = List()

  /**
    * matchFunction is still under specification, will implement it after requirement finalized.
    */
  var matchFunction: String = _
  var isPK: Boolean = _
}