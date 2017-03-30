package org.apache.griffin.config.params

sealed class DqType(val tp: String)

case object AccuracyType extends DqType("accuracy")
case object ProfileType extends DqType("profile")
case object ValidityType extends DqType("validity")

object DqType {
  def parse(tp: String): DqType = new DqType(tp)
}