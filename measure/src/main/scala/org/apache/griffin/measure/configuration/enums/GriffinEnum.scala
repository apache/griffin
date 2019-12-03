package org.apache.griffin.measure.configuration.enums

trait GriffinEnum extends Enumeration {

  /**
   *
   * @param name Constant value in String
   * @return Enum constant value
   */
  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Value)

}
