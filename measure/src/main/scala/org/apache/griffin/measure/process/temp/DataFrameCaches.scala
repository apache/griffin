package org.apache.griffin.measure.process.temp

import org.apache.griffin.measure.log.Loggable
import org.apache.spark.sql.DataFrame

import scala.collection.concurrent.{TrieMap, Map => ConcMap}

object DataFrameCaches extends Loggable {

  final val _global = "_global"

  private val caches: ConcMap[String, Map[String, DataFrame]] = TrieMap[String, Map[String, DataFrame]]()
  private val trashCaches: ConcMap[String, Seq[DataFrame]] = TrieMap[String, Seq[DataFrame]]()

  private def trashDataFrame(key: String, df: DataFrame): Unit = {
    trashCaches.get(key) match {
      case Some(seq) => {
        val suc = trashCaches.replace(key, seq, seq :+ df)
        if (!suc) trashDataFrame(key, df)
      }
      case _ => {
        val oldOpt = trashCaches.putIfAbsent(key, Seq[DataFrame](df))
        if (oldOpt.nonEmpty) trashDataFrame(key, df)
      }
    }
  }
  private def trashDataFrames(key: String, dfs: Seq[DataFrame]): Unit = {
    trashCaches.get(key) match {
      case Some(seq) => {
        val suc = trashCaches.replace(key, seq, seq ++ dfs)
        if (!suc) trashDataFrames(key, dfs)
      }
      case _ => {
        val oldOpt = trashCaches.putIfAbsent(key, dfs)
        if (oldOpt.nonEmpty) trashDataFrames(key, dfs)
      }
    }
  }

  def cacheDataFrame(key: String, name: String, df: DataFrame): Unit = {
    println(s"try to cache df ${name}")
    caches.get(key) match {
      case Some(mp) => {
        mp.get(name) match {
          case Some(odf) => {
            val suc = caches.replace(key, mp, mp + (name -> df))
            if (suc) {
              println(s"cache after replace old df")
              df.cache
              trashDataFrame(key, odf)
            } else {
              cacheDataFrame(key, name, df)
            }
          }
          case _ => {
            val suc = caches.replace(key, mp, mp + (name -> df))
            if (suc) {
              println(s"cache after replace no old df")
              df.cache
            } else {
              cacheDataFrame(key, name, df)
            }
          }
        }
      }
      case _ => {
        val oldOpt = caches.putIfAbsent(key, Map[String, DataFrame]((name -> df)))
        if (oldOpt.isEmpty) {
          println(s"cache after put absent")
          df.cache
        } else {
          cacheDataFrame(key, name, df)
        }
      }
    }
  }
  def cacheGlobalDataFrame(name: String, df: DataFrame): Unit = {
    cacheDataFrame(_global, name, df)
  }

  def uncacheDataFrames(key: String): Unit = {
    caches.remove(key) match {
      case Some(mp) => {
        trashDataFrames(key, mp.values.toSeq)
      }
      case _ => {}
    }
  }
  def uncacheGlobalDataFrames(): Unit = {
    uncacheDataFrames(_global)
  }

  def clearTrashDataFrames(key: String): Unit = {
    trashCaches.remove(key) match {
      case Some(seq) => seq.foreach(_.unpersist)
      case _ => {}
    }
  }
  def clearGlobalTrashDataFrames(): Unit = {
    clearTrashDataFrames(_global)
  }

  def getDataFrames(key: String): Map[String, DataFrame] = {
    caches.get(key) match {
      case Some(mp) => mp
      case _ => Map[String, DataFrame]()
    }
  }
  def getGlobalDataFrames(): Map[String, DataFrame] = {
    getDataFrames(_global)
  }



}
