/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */
package org.apache.griffin.measure.cache

import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

case class ZKInfoCache(config: Map[String, Any], metricName: String) extends InfoCache {

  val Hosts = "hosts"
  val ParentPath = "parent.path"
  val Mode = "mode"
  val InitClear = "init.clear"
  val CloseClear = "close.clear"
  val LockPath = "lock.path"

  val PersistRegex = """^(?i)persist$""".r
  val EphemeralRegex = """^(?i)ephemeral$""".r

  final val separator = "/"

  val hosts = config.getOrElse(Hosts, "").toString
  val parentPath = config.getOrElse(ParentPath, separator).toString
  val mode: CreateMode = config.get(Mode) match {
    case Some(s: String) => s match {
      case PersistRegex() => CreateMode.PERSISTENT
      case EphemeralRegex() => CreateMode.EPHEMERAL
      case _ => CreateMode.PERSISTENT
    }
    case _ => CreateMode.PERSISTENT
  }
  val initClear = config.get(InitClear) match {
    case Some(b: Boolean) => b
    case _ => true
  }
  val closeClear = config.get(CloseClear) match {
    case Some(b: Boolean) => b
    case _ => false
  }
  val lockPath = config.getOrElse(LockPath, "lock").toString

  private val client: CuratorFramework = CuratorFrameworkFactory.newClient(
    Hosts, new ExponentialBackoffRetry(1000, 3)).usingNamespace(cacheParentPath)

  def init(): Unit = {
    client.start()
    if (initClear) {
      clearInfo
    }
  }

  def available(): Boolean = {
    client.getState match {
      case CuratorFrameworkState.STARTED => true
      case _ => false
    }
  }

  def close(): Unit = {
    if (closeClear) {
      clearInfo
    }
    client.close()
  }

  def cacheInfo(info: Map[String, String]): Boolean = {
    info.foldLeft(true) { (rs, pair) =>
      val (k, v) = pair
      createOrUpdate(path(k), v) && rs
    }
  }

  def readInfo(keys: Iterable[String]): Map[String, String] = {
    keys.flatMap { key =>
      read(key) match {
        case Some(v) => Some((key, v))
        case _ => None
      }
    }.toMap
  }

  def deleteInfo(keys: Iterable[String]): Unit = {
    keys.foreach { key => delete(path(key)) }
  }

  def clearInfo(): Unit = {
    delete("/")
  }

  def genLock(s: String): ZKCacheLock = {
    val lpt = if (s.isEmpty) path(lockPath) else validHeadPath(lockPath) + s
    ZKCacheLock(new InterProcessMutex(client, lpt))
  }

  private def path(k: String): String = {
    if (k.startsWith(separator)) k else separator + k
  }

  private def validHeadPath(head: String): String = {
    val hd = if (head.startsWith(separator)) head else separator + head
    if (hd.endsWith(separator)) hd else hd + separator
  }

  private def cacheParentPath: String = {
    validHeadPath(parentPath) + metricName
  }

  private def createOrUpdate(path: String, content: String): Boolean = {
    if (checkExists(path)) {
      update(path, content)
    } else {
      create(path, content)
    }
  }

  private def create(path: String, content: String): Boolean = {
    try {
      client.create().creatingParentsIfNeeded().withMode(mode)
        .forPath(path, content.getBytes("utf-8"))
      true
    } catch {
      case e: Throwable => {
        error(s"create ( ${path} -> ${content} ) error: ${e.getMessage}")
        false
      }
    }
  }

  private def update(path: String, content: String): Boolean = {
    try {
      client.setData().forPath(path, content.getBytes("utf-8"))
      true
    } catch {
      case e: Throwable => {
        error(s"update ( ${path} -> ${content} ) error: ${e.getMessage}")
        false
      }
    }
  }

  private def read(path: String): Option[String] = {
    try {
      Some(new String(client.getData().forPath(path), "utf-8"))
    } catch {
      case e: Throwable => {
        error(s"read ${path} error: ${e.getMessage}")
        None
      }
    }
  }

  private def delete(path: String): Unit = {
    try {
      client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path)
    } catch {
      case e: Throwable => error(s"delete ${path} error: ${e.getMessage}")
    }
  }

  private def checkExists(path: String): Boolean = {
    try {
      client.checkExists().forPath(path) != null
    } catch {
      case e: Throwable => {
        error(s"check exists ${path} error: ${e.getMessage}")
        false
      }
    }
  }

}
