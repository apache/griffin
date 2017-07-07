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
package org.apache.griffin.measure.rule

object SchemaValueCombineUtil {

  // Map[String, List[(List[String], T)]]: Map[key, List[(path, value)]]
  def cartesian[T](valuesMap: Map[String, List[(List[String], T)]]): List[Map[String, T]] = {
    val fieldsList: List[(String, List[(List[String], T)])] = valuesMap.toList

    // wrong algorithm: assume the lists have same size
//    val minSize = fieldsList.map(_._2.size).min
//    val idxes = (0 to (minSize - 1)).toList
//    idxes.map { idx =>
//      fieldsList.foldLeft(Map[String, T]()) { (map, pair) =>
//        val (key, value) = pair
//        map + (key -> value(idx)._2)
//      }
//    }

    // following is correct algorithm

    // List[key, List[(path, value)]] to List[(path, (key, value))]
    val valueList: List[(List[String], (String, T))] = fieldsList.flatMap { fields =>
      val (key, list) = fields
      list.map { pv =>
        val (path, value) = pv
        (path, (key, value))
      }
    }

    // 1. generate tree from value list, and return root node
    val root = TreeUtil.genRootTree(valueList)

    // 2. deep first visit tree from root, merge datas into value map list
    val valueMapList: List[Map[String, _]] = TreeUtil.mergeDatasIntoMap(root, Nil)

    // 3. simple change
    valueMapList.map { mp =>
      mp.map { kv =>
        val (k, v) = kv
        (k, v.asInstanceOf[T])
      }
    }

  }

//  def cartesianTest[T](valuesMap: Map[String, List[(List[String], T)]]): Unit = {
//    val fieldsList: List[(String, List[(List[String], T)])] = valuesMap.toList
//
//    // List[key, List[(path, value)]] to List[(path, (key, value))]
//    val valueList: List[(List[String], (String, T))] = fieldsList.flatMap { fields =>
//      val (key, list) = fields
//      list.map { pv =>
//        val (path, value) = pv
//        (path, (key, value))
//      }
//    }
//
//    // 1. generate tree from value list, and return root node
//    val root = TreeUtil.genRootTree(valueList)
//
//    // 2. deep first visit tree from root, merge datas into value map list
//    val valueMapList: List[Map[String, _]] = TreeUtil.mergeDatasIntoMap(root, Nil)
//
//    valueMapList.foreach(println)
//
//  }


  case class TreeNode(key: String, var datas: List[(String, _)]) {
    var children = List[TreeNode]()
    def addChild(node: TreeNode): Unit = children = children :+ node
    def mergeSelf(node: TreeNode): Unit = datas = datas ::: node.datas
  }

  object TreeUtil {
    private def genTree(path: List[String], datas: List[(String, _)]): Option[TreeNode] = {
      path match {
        case Nil => None
//        case head :: Nil => Some(TreeNode(datas))
        case head :: tail => {
          genTree(tail, datas) match {
            case Some(child) => {
              val curNode = TreeNode(head, Nil)
              curNode.addChild(child)
              Some(curNode)
            }
            case _ => Some(TreeNode(head, datas))
          }
        }
      }
    }

    private def mergeTrees(trees: List[TreeNode], newTreeOpt: Option[TreeNode]): List[TreeNode] = {
      newTreeOpt match {
        case Some(newTree) => {
          trees.find(tree => tree.key == newTree.key) match {
            case Some(tree) => {
              // children merge
              for (child <- newTree.children) {
                tree.children = mergeTrees(tree.children, Some(child))
              }
              // self data merge
              tree.mergeSelf(newTree)
              trees
            }
            case _ => trees :+ newTree
          }
        }
        case _ => trees
      }
    }

    private def root(): TreeNode = TreeNode("", Nil)

    def genRootTree(values: List[(List[String], (String, _))]): TreeNode = {
      val rootNode = root()
      val nodeOpts = values.map(value => genTree(value._1, value._2 :: Nil))
      rootNode.children = nodeOpts.foldLeft(List[TreeNode]()) { (trees, treeOpt) =>
        mergeTrees(trees, treeOpt)
      }
      rootNode
    }

    private def add(mapList1: List[Map[String, _]], mapList2: List[Map[String, _]]):  List[Map[String, _]] = {
      mapList1 ::: mapList2
    }
    private def multiply(mapList1: List[Map[String, _]], mapList2: List[Map[String, _]]):  List[Map[String, _]] = {
      mapList1.flatMap { map1 =>
        mapList2.map { map2 =>
          map1 ++ map2
        }
      }
    }

    private def keysList(mapList: List[Map[String, _]]): List[String] = {
      val keySet = mapList match {
        case Nil => Set[String]()
        case head :: _ => head.keySet
      }
      keySet.toList
    }

    def mergeDatasIntoMap(root: TreeNode, mapDatas: List[Map[String, _]]): List[Map[String, _]] = {
      val childrenKeysMapDatas = root.children.foldLeft(Map[List[String], List[Map[String, _]]]()) { (keysMap, child) =>
        val childMdts = mergeDatasIntoMap(child, List[Map[String, _]]())
        val keys = keysList(childMdts)
        val afterList = keysMap.get(keys) match {
          case Some(list) => add(list, childMdts)
          case _ => childMdts
        }
        keysMap + (keys -> afterList)
      }
      val childrenMergeMaps = childrenKeysMapDatas.values.foldLeft(List[Map[String, _]]()) { (originList, list) =>
        originList match {
          case Nil => list
          case _ => multiply(originList, list)
        }
      }
      mergeNodeChildrenDatasIntoMap(root, childrenMergeMaps)
    }

    private def mergeNodeChildrenDatasIntoMap(node: TreeNode, mapDatas: List[Map[String, _]]): List[Map[String, _]] = {
      val datas: List[(String, (String, Any))] = node.children.flatMap { child =>
        child.datas.map(dt => (dt._1, (child.key, dt._2)))
      }
      val childrenDataKeys: Set[String] = datas.map(_._1).toSet
      val childrenDataLists: Map[String, List[(String, _)]] = datas.foldLeft(childrenDataKeys.map(k => (k, List[(String, _)]())).toMap) { (maps, data) =>
        maps.get(data._1) match {
          case Some(list) => maps + (data._1 -> (list :+ data._2))
          case _ => maps
        }
      }

      // multiply different key datas
      childrenDataLists.foldLeft(mapDatas) { (mdts, klPair) =>
        val (key, list) = klPair
        mdts match {
          case Nil => list.map(pr => Map[String, Any]((key -> pr._2)))
          case _ => {
            list.flatMap { kvPair =>
              val (path, value) = kvPair
              mdts.map { mp =>
                mp + (key -> value)
              }
            }
          }
        }
      }

    }
  }




}
