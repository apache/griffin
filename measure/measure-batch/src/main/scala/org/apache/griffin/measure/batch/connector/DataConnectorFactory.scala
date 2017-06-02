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
package org.apache.griffin.measure.batch.connector

import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.rule.RuleExprs
import org.apache.griffin.measure.batch.rule.expr._
import org.apache.spark.sql.SQLContext

import scala.util.Try

object DataConnectorFactory {

  val HiveRegex = """^(?i)hive$""".r
  val AvroRegex = """^(?i)avro$""".r

  def getDataConnector(sqlContext: SQLContext,
                       dataConnectorParam: DataConnectorParam,
                       ruleExprs: RuleExprs,
                       globalFinalCacheMap: Map[String, Any]
                      ): Try[DataConnector] = {
    val conType = dataConnectorParam.conType
    val version = dataConnectorParam.version
    Try {
      conType match {
        case HiveRegex() => HiveDataConnector(sqlContext, dataConnectorParam.config, ruleExprs, globalFinalCacheMap)
        case AvroRegex() => AvroDataConnector(sqlContext, dataConnectorParam.config, ruleExprs, globalFinalCacheMap)
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

}
