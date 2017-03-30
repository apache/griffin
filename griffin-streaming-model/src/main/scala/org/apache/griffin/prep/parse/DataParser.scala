package org.apache.griffin.prep.parse

trait DataParser extends Serializable {

  type In
  type Out

  def parse(data: In): Seq[Out]

}
