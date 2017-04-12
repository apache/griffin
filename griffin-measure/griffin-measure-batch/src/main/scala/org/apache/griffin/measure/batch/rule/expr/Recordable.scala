package org.apache.griffin.measure.batch.rule.expr


trait Recordable extends Serializable {

  val recordName: String

  protected def value2RecordString(v: Any): String = {
    v match {
      case s: String => s"'${s}'"
      case a => s"${a}"
    }
  }

}
