package org.apache.griffin.dump

import org.apache.griffin.utils.HdfsUtil
import org.apache.spark.rdd.RDD

object HdfsFileDumpUtil {

  val sepCount = 5000

  private def suffix(i: Long): String = {
    if (i == 0) "" else s".${i}"
  }

  def splitRdd[T](rdd: RDD[T])(implicit m: Manifest[T]): RDD[(Long, Iterable[T])] = {
    val indexRdd = rdd.zipWithIndex
    indexRdd.map(p => ((p._2 / sepCount), p._1)).groupByKey()
  }

  private def directDump(path: String, list: Iterable[String], lineSep: String): Unit = {
    // collect and save
    val strRecords = list.mkString(lineSep)
    // save into hdfs
    HdfsUtil.writeContent(path, strRecords)
  }

  def dump(path: String, recordsRdd: RDD[String], lineSep: String): Unit = {
    val groupedRdd = splitRdd(recordsRdd)

    // dump
    groupedRdd.foreach { pair =>
      val (idx, list) = pair
      val filePath = path + suffix(idx)
      directDump(filePath, list, lineSep)
    }
  }

}
