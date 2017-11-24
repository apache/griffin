/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.persist

import org.mongodb.scala._
import org.apache.griffin.measure.utils.ParamUtil._
import org.mongodb.scala.model.{Filters, UpdateOptions, Updates}
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.Future


case class MongoPersist(config: Map[String, Any], metricName: String, timeStamp: Long) extends Persist {

  MongoConnection.init(config)

//  val Url = "url"
//  val Database = "database"
//  val Collection = "collection"
//  val _ID = "_id"
//
//  val IdGen = "id.gen"

//  def mongoConf(cfg: Map[String, Any]): MongoConf = {
//    MongoConf(
//      s"mongodb://${config.getString(Url, "")}",
//      config.getString(Database, ""),
//      config.getString(Collection, ""),
//      config.getString(_ID, "")
//    )
//  }
//  def mongoCollection(mongoConf: MongoConf): MongoCollection[Document] = {
//    val mongoClient: MongoClient = MongoClient(mongoConf.url)
//    val database: MongoDatabase = mongoClient.getDatabase(mongoConf.database)
//    database.getCollection(mongoConf.collection)
//  }

//  val dataConf = mongoConf(config)
//  val idGenOpt = config.getParamMapOpt(IdGen)
//  val idGenConfOpt = idGenOpt.map(mongoConf(_))
//
//  val dataCollection: MongoCollection[Document] = mongoCollection(dataConf)
//  val idGenCollectionOpt: Option[MongoCollection[Document]] = idGenConfOpt.map(mongoCollection(_))

  val _Value = "value"

  def available(): Boolean = MongoConnection.dataConf.available

  def start(msg: String): Unit = {}
  def finish(): Unit = {}

  def log(rt: Long, msg: String): Unit = {}

  def persistRecords(records: Iterable[String], name: String): Unit = {}

  def persistMetrics(metrics: Map[String, Any]): Unit = {
    mongoInsert(metrics)
  }

  private val filter = Filters.and(
    Filters.eq("metricName", metricName),
    Filters.eq("timestamp", timeStamp)
  )
  private val idKey = MongoConnection.idGenConfOpt match {
    case Some(conf) => conf._id
    case _ => ""
  }
  private val idFilter = Filters.eq(MongoConnection._ID, idKey)
  private val idUpdate = Updates.inc(idKey, 1)
  private def mongoInsert(dataMap: Map[String, Any]): Unit = {
    try {
      MongoConnection.getIdGenCollectionOpt match {
        case Some(idc) => {
          idc.findOne()
          idc.findOneAndUpdate(idFilter, idUpdate)
            .subscribe((result: Document) => {
              val id = result.getLong(idKey)
              mongoInsert(dataMap, Some(id))
            })
//          mongoInsert(dataMap, None)
        }
        case _ => {
          mongoInsert(dataMap, None)
        }
      }
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }
  private def mongoInsert(dataMap: Map[String, Any], idOpt: Option[Long]): Unit = {
    try {
      val update = idOpt match {
        case Some(id) => Updates.combine(
          Updates.set(_Value, dataMap),
          Updates.set(MongoConnection._ID, id)
        )
        case _ => Updates.combine(
          Updates.set(_Value, dataMap)
        )
      }
      def func(): (Long, Future[UpdateResult]) = {
        (timeStamp, MongoConnection.getDataCollection.updateOne(
          filter, update, UpdateOptions().upsert(true)).toFuture)
      }
      MongoThreadPool.addTask(func _, 10)
    } catch {
      case e: Throwable => error(e.getMessage)
    }
  }

}

case class MongoConf(url: String, database: String, collection: String, _id: String) {
  def available: Boolean = url.nonEmpty && database.nonEmpty && collection.nonEmpty
}

object MongoConnection {

  val Url = "url"
  val Database = "database"
  val Collection = "collection"
  val _ID = "_id"

  val IdGen = "id.gen"

  private var initialed = false

  var dataConf: MongoConf = null
  var idGenOpt: Option[Map[String, Any]] = null
  var idGenConfOpt: Option[MongoConf] = null

  private var dataCollection: MongoCollection[Document] = null
  private var idGenCollectionOpt: Option[MongoCollection[Document]] = null

  def getDataCollection = dataCollection
  def getIdGenCollectionOpt = idGenCollectionOpt

  def init(config: Map[String, Any]): Unit = {
    if (!initialed) {
      dataConf = mongoConf(config)
      idGenOpt = config.getParamMapOpt(IdGen)
      idGenConfOpt = idGenOpt.map(mongoConf(_))

      dataCollection = mongoCollection(dataConf)
      idGenCollectionOpt = idGenConfOpt.map(mongoCollection(_))

      initialed = true
    }
  }

  def mongoConf(cfg: Map[String, Any]): MongoConf = {
    MongoConf(
      s"mongodb://${cfg.getString(Url, "")}",
      cfg.getString(Database, ""),
      cfg.getString(Collection, ""),
      cfg.getString(_ID, "")
    )
  }
  def mongoCollection(mongoConf: MongoConf): MongoCollection[Document] = {
    val mongoClient: MongoClient = MongoClient(mongoConf.url)
    val database: MongoDatabase = mongoClient.getDatabase(mongoConf.database)
    database.getCollection(mongoConf.collection)
  }

}