package org.apache.griffin.measure.batch.rule.expr

trait Describable extends Serializable {

  val desc: String

  protected def describe(v: Any): String = {
    v match {
      case s: Describable => s"${s.desc}"
      case s: String => s"'${s}'"
      case a => s"${a}"
    }
  }

}
