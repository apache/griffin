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
package org.apache.griffin.measure.rule.func

import java.lang.reflect.Method

import org.apache.griffin.measure.log.Loggable

import scala.collection.mutable.{Map => MutableMap}

object FunctionUtil extends Loggable {

  val functionDefines: MutableMap[String, FunctionDefine] = MutableMap[String, FunctionDefine]()

  registerFunctionDefine(Array(classOf[DefaultFunctionDefine].getCanonicalName))

  def registerFunctionDefine(classes: Iterable[String]): Unit = {
    for (cls <- classes) {
      try {
        val clz: Class[_] = Class.forName(cls)
        if (clz.isAssignableFrom(classOf[FunctionDefine])) {
          functionDefines += (cls -> clz.newInstance.asInstanceOf[FunctionDefine])
        } else {
          warn(s"${cls} register fails: ${cls} is not sub class of ${classOf[FunctionDefine].getCanonicalName}")
        }
      } catch {
        case e: Throwable => warn(s"${cls} register fails: ${e.getMessage}")
      }
    }
  }

  def invoke(methodName: String, params: Array[Option[Any]]): Seq[Option[Any]] = {
    val paramTypes = params.map { param =>
      try {
        param match {
          case Some(v) => v.getClass
          case _ => classOf[UnKnown]
        }
      } catch {
        case e: Throwable => classOf[UnKnown]
      }
    }

    functionDefines.values.foldLeft(Nil: Seq[Option[Any]]) { (res, funcDef) =>
      if (res.isEmpty) {
        val clz = funcDef.getClass
        try {
          val method = clz.getMethod(methodName, paramTypes: _*)
          Seq(Some(method.invoke(funcDef, params: _*)))
        } catch {
          case e: Throwable => res
        }
      } else res
    }
  }

}

