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
package org.apache.griffin.measure.rule.expr

import org.apache.spark.sql.types.DataType

trait Expr extends Serializable with Describable with Cacheable with Calculatable {

  protected val _defaultId: String = ExprIdCounter.emptyId

  val _id = ExprIdCounter.genId(_defaultId)

  def dataType: DataType

  protected def getSubCacheExprs(ds: String): Iterable[Expr] = Nil
  final def getCacheExprs(ds: String): Iterable[Expr] = {
    if (cacheable(ds)) getSubCacheExprs(ds).toList :+ this else getSubCacheExprs(ds)
  }

  protected def getSubFinalCacheExprs(ds: String): Iterable[Expr] = Nil
  final def getFinalCacheExprs(ds: String): Iterable[Expr] = {
    if (cacheable(ds)) Nil :+ this else getSubFinalCacheExprs(ds)
  }

  protected def getSubPersistExprs(ds: String): Iterable[Expr] = Nil
  final def getPersistExprs(ds: String): Iterable[Expr] = {
    if (persistable(ds)) getSubPersistExprs(ds).toList :+ this else getSubPersistExprs(ds)
  }

  final def calculate(values: Map[String, Any]): Option[Any] = {
    values.get(_id) match {
      case Some(v) => Some(v)
      case _ => calculateOnly(values)
    }
  }
  protected def calculateOnly(values: Map[String, Any]): Option[Any]

}

