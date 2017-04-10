package org.apache.griffin.measure.batch.dsl.expr


object ExprIdCounter {

  private var curId: Long = 0L

  def genId(head: String): String = {
    curId += 1
    s"${head}#${curId}"
  }

}
