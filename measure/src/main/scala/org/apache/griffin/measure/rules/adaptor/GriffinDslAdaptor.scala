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
package org.apache.griffin.measure.rules.adaptor

import org.apache.griffin.measure.config.params.user.RuleParam
import org.apache.griffin.measure.rules.dsl._
import org.apache.griffin.measure.rules.dsl.analyzer._
import org.apache.griffin.measure.rules.dsl.expr.Expr
import org.apache.griffin.measure.rules.dsl.parser.GriffinDslParser
import org.apache.griffin.measure.rules.step._

case class GriffinDslAdaptor(dataSourceNames: Seq[String], functionNames: Seq[String]) extends RuleAdaptor {

  object StepInfo {
    val _Name = "name"
    val _PersistType = "persist.type"
    def getNameOpt(param: Map[String, Any]): Option[String] = param.get(_Name).flatMap(a => Some(a.toString))
    def getPersistType(param: Map[String, Any]): PersistType = PersistType(param.getOrElse(_PersistType, "").toString)
  }
  object AccuracyInfo {
    val _Source = "source"
    val _Target = "target"
    val _MissRecord = "miss.record"
    val _MissCount = "miss.count"
    val _TotalCount = "total.count"
    def getNameOpt(param: Map[String, Any], key: String): Option[String] = param.get(key).flatMap(a => Some(a.toString))
    def resultName(param: Map[String, Any], key: String): String = {
      val nameOpt = param.get(key) match {
        case Some(prm: Map[String, Any]) => StepInfo.getNameOpt(prm)
        case _ => None
      }
      nameOpt.getOrElse(key)
    }
    def resultPersistType(param: Map[String, Any], key: String): PersistType = {
      param.get(key) match {
        case Some(prm: Map[String, Any]) => StepInfo.getPersistType(prm)
        case _ => NonePersistType
      }
    }
  }

  val _dqType = "dq.type"
  val _details = "details"

  val filteredFunctionNames = functionNames.filter { fn =>
    fn.matches("""^[a-zA-Z_]\w*$""")
  }
  val parser = GriffinDslParser(dataSourceNames, filteredFunctionNames)

  protected def getDqType(param: Map[String, Any]) = DqType(param.getOrElse(_dqType, "").toString)
  protected def getDetails(param: Map[String, Any]) = {
    param.get(_details) match {
      case Some(p: Map[String, Any]) => p
      case _ => Map[String, Any]()
    }
  }

  def genRuleStep(param: Map[String, Any]): Seq[RuleStep] = {
    GriffinDslStep(getName(param), getRule(param), getDqType(param), getDetails(param)) :: Nil
  }

  def getTempSourceNames(param: Map[String, Any]): Seq[String] = {
    val dqType = getDqType(param)
    param.get(_name) match {
      case Some(name) => {
        dqType match {
          case AccuracyType => {
            Seq[String](
              AccuracyInfo.resultName(param, AccuracyInfo._MissRecord),
              AccuracyInfo.resultName(param, AccuracyInfo._MissCount),
              AccuracyInfo.resultName(param, AccuracyInfo._TotalCount)
            )
          }
          case ProfilingType => {
            Nil
          }
          case TimelinessType => {
            Nil
          }
          case _ => Nil
        }
      }
      case _ => Nil
    }
  }

  override def adaptConcreteRuleStep(ruleStep: RuleStep): Seq[ConcreteRuleStep] = {
    ruleStep match {
      case rs @ GriffinDslStep(_, rule, _, _) => {
        val exprOpt = try {
          val result = parser.parseAll(parser.rootExpression, rule)
          if (result.successful) Some(result.get)
          else {
            warn(s"adapt concrete rule step warn: ${rule}")
            None
          }
        } catch {
          case e: Throwable => {
            error(s"adapt concrete rule step error: ${e.getMessage}")
            None
          }
        }

        exprOpt match {
          case Some(expr) => {
            transConcreteRuleSteps(rs, expr)
          }
          case _ => Nil
        }
      }
      case _ => Nil
    }
  }

  private def transConcreteRuleSteps(ruleStep: GriffinDslStep, expr: Expr): Seq[ConcreteRuleStep] = {
    ruleStep.dqType match {
      case AccuracyType => {
        val sourceName = AccuracyInfo.getNameOpt(ruleStep.details, AccuracyInfo._Source) match {
          case Some(name) => name
          case _ => dataSourceNames.head
        }
        val targetName = AccuracyInfo.getNameOpt(ruleStep.details, AccuracyInfo._Target) match {
          case Some(name) => name
          case _ => dataSourceNames.tail.head
        }
        val analyzer = AccuracyAnalyzer(expr, sourceName, targetName)

        // 1. miss record
        val missRecordSql = {
          val selClause = analyzer.sourceSelectionExprs.map { sel =>
            val alias = sel.alias match {
              case Some(a) => s" AS ${a}"
              case _ => ""
            }
            s"${sel.desc}${alias}"
          }.mkString(", ")

          s"SELECT ${selClause} FROM ${sourceName} LEFT JOIN ${targetName}"
        }
        println(missRecordSql)

        // 2. miss count
        val missCountSql = {
          ""
        }

        // 3. total count
        val totalCountSql = {
          ""
        }

        Nil
      }
      case ProfilingType => {
        Nil
      }
      case TimelinessType => {
        Nil
      }
      case _ => Nil
    }
  }

}
