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
package org.apache.griffin.measure.rule.dsl.expr

trait TreeNode extends Serializable {

  var children = Seq[TreeNode]()

  def addChild(expr: TreeNode) = { children :+= expr }
  def addChildren(exprs: Seq[TreeNode]) = { children ++= exprs }

  def preOrderTraverseDepthFirst[T, A <: TreeNode](z: T)(seqOp: (A, T) => T, combOp: (T, T) => T): T = {
    if (this.isInstanceOf[A]) {
      val tv = seqOp(this.asInstanceOf[A], z)
      children.foldLeft(combOp(z, tv)) { (ov, tn) =>
        combOp(ov, tn.preOrderTraverseDepthFirst(z)(seqOp, combOp))
      }
    } else z
  }
  def postOrderTraverseDepthFirst[T, A <: TreeNode](z: T)(seqOp: (A, T) => T, combOp: (T, T) => T): T = {
    if (this.isInstanceOf[A]) {
      val cv = children.foldLeft(z) { (ov, tn) =>
        combOp(ov, tn.postOrderTraverseDepthFirst(z)(seqOp, combOp))
      }
      combOp(z, seqOp(this.asInstanceOf[A], cv))
    } else z
  }

}
