///*
//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.
//*/
//package org.apache.griffin.measure.algo.core
//
//import org.apache.griffin.measure.config.params.user.EvaluateRuleParam
//import org.apache.griffin.measure.rule.expr._
//import org.apache.griffin.measure.rule.{RuleAnalyzer, RuleFactory}
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//import org.scalatest.PrivateMethodTester
//
//@RunWith(classOf[JUnitRunner])
//class AccuracyCoreTest extends FunSuite with Matchers with BeforeAndAfter with PrivateMethodTester {
//
//  def findExprId(exprs: Iterable[Expr], desc: String): String = {
//    exprs.find(_.desc == desc) match {
//      case Some(expr) => expr._id
//      case _ => ""
//    }
//  }
//
//  test ("match data success") {
//    val rule = "$source.name = $target.name AND $source.age < $target.age"
//    val evaluateRuleParam = EvaluateRuleParam(1.0, rule)
//    val ruleFactory = RuleFactory(evaluateRuleParam)
//    val statement = ruleFactory.generateRule
//    val ruleAnalyzer = RuleAnalyzer(statement)
//
//    val sourcePersistExprs = ruleAnalyzer.sourceRuleExprs.persistExprs
//    val targetPersistExprs = ruleAnalyzer.targetRuleExprs.persistExprs
//
//    val source = (Map[String, Any](
//      (findExprId(sourcePersistExprs, "$source['name']") -> "jack"),
//      (findExprId(sourcePersistExprs, "$source['age']") -> 26)
//    ), Map[String, Any]())
//    val target = (Map[String, Any](
//      (findExprId(targetPersistExprs, "$target['name']") -> "jack"),
//      (findExprId(targetPersistExprs, "$target['age']") -> 27)
//    ), Map[String, Any]())
//
//    val matchData = PrivateMethod[(Boolean, Map[String, Any])]('matchData)
//    val result = AccuracyCore invokePrivate matchData(source, target, ruleAnalyzer)
//    result._1 should be (true)
//    result._2.size should be (0)
//  }
//
//  test ("match data fail") {
//    val rule = "$source.name = $target.name AND $source.age = $target.age"
//    val evaluateRuleParam = EvaluateRuleParam(1.0, rule)
//    val ruleFactory = RuleFactory(evaluateRuleParam)
//    val statement = ruleFactory.generateRule
//    val ruleAnalyzer = RuleAnalyzer(statement)
//
//    val sourcePersistExprs = ruleAnalyzer.sourceRuleExprs.persistExprs
//    val targetPersistExprs = ruleAnalyzer.targetRuleExprs.persistExprs
//
//    val source = (Map[String, Any](
//      (findExprId(sourcePersistExprs, "$source['name']") -> "jack"),
//      (findExprId(sourcePersistExprs, "$source['age']") -> 26)
//    ), Map[String, Any]())
//    val target = (Map[String, Any](
//      (findExprId(targetPersistExprs, "$target['name']") -> "jack"),
//      (findExprId(targetPersistExprs, "$target['age']") -> 27)
//    ), Map[String, Any]())
//
//    val matchData = PrivateMethod[(Boolean, Map[String, Any])]('matchData)
//    val result = AccuracyCore invokePrivate matchData(source, target, ruleAnalyzer)
//    result._1 should be (false)
//    result._2.size shouldNot be (0)
//  }
//
//}
