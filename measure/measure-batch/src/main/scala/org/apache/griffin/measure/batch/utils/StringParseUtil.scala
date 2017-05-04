package org.apache.griffin.measure.batch.utils

object StringParseUtil {

  def sepStrings(str: String, sep: String): Iterable[String] = {
    val strings = str.split(sep)
    strings.map(_.trim)
  }

}
