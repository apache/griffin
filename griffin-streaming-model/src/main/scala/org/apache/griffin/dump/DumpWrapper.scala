package org.apache.griffin.dump

case class DumpWrapper(infos: Iterable[DumpWrapperInfo]) extends Serializable {

  def wrapSchema(): List[(String, String)] = {
    infos.map { info =>
      (info.key, info.tp)
    }.toList
  }

  def wrapValues[T](data: Map[String, T]): List[Option[T]] = {
    infos.map { i =>
      data.get(i.key)
    }.toList
  }

  def wrapKeys(): List[String] = {
    infos.map(_.key).toList
  }

}
