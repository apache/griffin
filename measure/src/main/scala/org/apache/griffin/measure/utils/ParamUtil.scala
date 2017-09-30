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
package org.apache.griffin.measure.utils

object ParamUtil {

  implicit class ParamMap(params: Map[String, Any]) {
    def getAny(key: String, defValue: Any): Any = {
      params.get(key) match {
        case Some(v) => v
        case _ => defValue
      }
    }

    def getAnyRef[T](key: String, defValue: T)(implicit m: Manifest[T]): T = {
      params.get(key) match {
        case Some(v: T) => v
        case _ => defValue
      }
    }

    def getString(key: String, defValue: String): String = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toString
          case Some(v) => v.toString
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getByte(key: String, defValue: Byte): Byte = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toByte
          case Some(v: Byte) => v.toByte
          case Some(v: Short) => v.toByte
          case Some(v: Int) => v.toByte
          case Some(v: Long) => v.toByte
          case Some(v: Float) => v.toByte
          case Some(v: Double) => v.toByte
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getShort(key: String, defValue: Short): Short = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toShort
          case Some(v: Byte) => v.toShort
          case Some(v: Short) => v.toShort
          case Some(v: Int) => v.toShort
          case Some(v: Long) => v.toShort
          case Some(v: Float) => v.toShort
          case Some(v: Double) => v.toShort
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getInt(key: String, defValue: Int): Int = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toInt
          case Some(v: Byte) => v.toInt
          case Some(v: Short) => v.toInt
          case Some(v: Int) => v.toInt
          case Some(v: Long) => v.toInt
          case Some(v: Float) => v.toInt
          case Some(v: Double) => v.toInt
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getLong(key: String, defValue: Long): Long = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toLong
          case Some(v: Byte) => v.toLong
          case Some(v: Short) => v.toLong
          case Some(v: Int) => v.toLong
          case Some(v: Long) => v.toLong
          case Some(v: Float) => v.toLong
          case Some(v: Double) => v.toLong
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getFloat(key: String, defValue: Float): Float = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toFloat
          case Some(v: Byte) => v.toFloat
          case Some(v: Short) => v.toFloat
          case Some(v: Int) => v.toFloat
          case Some(v: Long) => v.toFloat
          case Some(v: Float) => v.toFloat
          case Some(v: Double) => v.toFloat
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getDouble(key: String, defValue: Double): Double = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toDouble
          case Some(v: Byte) => v.toDouble
          case Some(v: Short) => v.toDouble
          case Some(v: Int) => v.toDouble
          case Some(v: Long) => v.toDouble
          case Some(v: Float) => v.toDouble
          case Some(v: Double) => v.toDouble
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }

    def getBoolean(key: String, defValue: Boolean): Boolean = {
      try {
        params.get(key) match {
          case Some(v: String) => v.toBoolean
          case _ => defValue
        }
      } catch {
        case _: Throwable => defValue
      }
    }
  }

}
