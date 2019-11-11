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
package org.apache.griffin.measure.datasource.connector.batch

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.utils.HdfsUtil
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * A batch data connector for file based sources which allows support various
 * file based data sources like Parquet, CSV, TSV, ORC etc.
 * Local files can also be read by prepending `file://` namespace.
 *
 * Currently supported formats like Parquet, ORC, AVRO, Text and Delimited types like CSV, TSV etc.
 *
 * Supported Configurations:
 *  - format : [[String]] specifying the type of file source (parquet, orc, etc.). Default: parquet
 *  - paths : [[Seq]] specifying the paths to be read
 *  - options : [[Map]] of format specific options
 *  - skipOnError : [[Boolean]] specifying where to continue execution if one or more paths are invalid.
 *  - schema : [[Seq]] of {colName, colType and isNullable} given as key value pairs. If provided, this can
 * help skip the schema inference step for some underlying data sources.
 */

case class FileBasedDataConnector(@transient sparkSession: SparkSession,
                                  dcParam: DataConnectorParam,
                                  timestampStorage: TimestampStorage)
  extends BatchDataConnector {

  import FileBasedDataConnector._

  val config: Map[String, Any] = dcParam.getConfig
  var options: Map[String, String] = config.getParamStringMap(Options, Map.empty)
  var currentSchema: StructType = _

  var format: String = config.getString(Format, DefaultFormat).toLowerCase
  val paths: Seq[String] = config.getStringArr(Paths, Nil)
  val schemaSeq: Seq[Map[String, String]] = config.getAnyRef[Seq[Map[String, String]]](Schema, Nil)
  val skipErrorPaths: Boolean = config.getBoolean(SkipErrorPaths, defValue = false)

  assert(SupportedFormats.contains(format),
    s"Invalid format '$format' specified. Must be one of ${SupportedFormats.mkString("['", "', '", "']")}")

  if (format == "csv") validateCSVOptions()
  if (format == "tsv") format = "csv"

  /**
   * Builds a [[StructType]] from the given schema string provided as `Schema` config.
   *
   * @example
   * {"schema":[{"name":"user_id","type":"string","nullable":"true"},{"name":"age","type":"int","nullable":"false"}]}
   * {"schema":[{"name":"user_id","type":"decimal(5,2)","nullable":"true"}]}
   * {"schema":[{"name":"my_struct","type":"struct<f1:int,f2:string>","nullable":"true"}]}
   * @return
   */
  private def getUserDefinedSchema: StructType = {
    schemaSeq.foldLeft(new StructType())((currentStruct, fieldMap) => {
      val colName = fieldMap(ColName).toLowerCase
      val colType = fieldMap(ColType).toLowerCase
      val isNullable = Try(fieldMap(IsNullable).toLowerCase.toBoolean).getOrElse(true)

      currentStruct.add(colName, colType, isNullable)
    })
  }

  private def validateCSVOptions(): Unit = {
    if (options.contains(Header) && config.contains(Schema)) {
      griffinLogger.warn(s"Both $Options.$Header and $Schema were provided. Defaulting to provided $Schema")
      options = options - Header
    }

    if (!options.contains(Header) && !config.contains(Schema)) {
      throw new IllegalArgumentException(s"Either '$Header' must be set in '$Options' or '$Schema' must be set.")
    }

    if (config.contains(Schema)) {
      if (schemaSeq.isEmpty) throw new IllegalStateException("Invalid Schema specified")
      else currentSchema = Try(getUserDefinedSchema) match {
        case Success(structType) if structType.fields.nonEmpty => structType
        case Failure(e) => throw new IllegalStateException("Unable to create schema from specification", e)
        case _ => throw new IllegalStateException("Unable to create schema from specification")
      }
    }
  }

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val validPaths = getValidPaths(paths, skipErrorPaths)

    val dfOpt = {
      val dfOpt = Some(
        sparkSession.read
          .options(options)
          .format(format)
          .schema(currentSchema)
          .load(validPaths: _*)

      )
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    }

    (dfOpt, TimeRange(ms, readTmst(ms)))
  }
}

object FileBasedDataConnector extends Loggable {
  private val Format: String = "format"
  private val Paths: String = "paths"
  private val Options: String = "options"
  private val SkipErrorPaths: String = "skipErrorPaths"
  private val Schema: String = "schema"
  private val Header: String = "header"

  private val ColName: String = "name"
  private val ColType: String = "type"
  private val IsNullable: String = "nullable"

  private val DefaultFormat: String = SQLConf.DEFAULT_DATA_SOURCE_NAME.defaultValueString
  private val SupportedFormats: Seq[String] = Seq("parquet", "orc", "avro", "text", "csv", "tsv")

  private def getValidPaths(paths: Seq[String], skipOnError: Boolean): Seq[String] = {
    val validPaths = paths.filter(path =>
      if (HdfsUtil.existPath(path)) true
      else {
        val msg = s"Path '$path' does not exist!"
        if (skipOnError) griffinLogger.error(msg)
        else throw new IllegalArgumentException(msg)

        false
      }
    )

    assert(validPaths.nonEmpty, "No paths were given for the data source.")
    validPaths
  }

}
