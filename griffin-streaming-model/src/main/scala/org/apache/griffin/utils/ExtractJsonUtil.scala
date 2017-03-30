package org.apache.griffin.utils

import scala.util.{Failure, Success, Try}


object ExtractJsonUtil {

  val JSON_REGEX = """^(?i)json$""".r
  val COUNT_REGEX = """^(?i)count$""".r
  val MAP_REGEX = """^\.(.+)$""".r
  val LIST_REGEX = """^\[(\d+)\]$""".r
  val ALL_LIST_REGEX = """^\[\*\]$""".r
  val FILTER_LIST_REGEX = """^\[\.(.+)=(.+)\]$""".r

  def extract(data: Option[Any], steps: List[String]): Option[Any] = {
    Try {
      steps match {
        case Nil => data
        case step :: tailSteps => {
          step match {
            case JSON_REGEX() => {
              data match {
//                case Some(line: String) => extract(Some(JsonUtil.toMap[Any](line)), tailSteps)
                case Some(line: String) => extract(Some(JsonUtil.toAnyMap(line)), tailSteps)
                case _ => {
                  throw new Exception(s"error: ${step} should be for string")
                }
              }
            }
            case MAP_REGEX(key) => {
              data match {
                case Some(map: Map[String, Any]) => extract(map.get(key), tailSteps)
                case _ => {
                  throw new Exception(s"error: ${step} should be for map")
                }
              }
            }
            case LIST_REGEX(index) => {
              data match {
                case Some(list: List[Any]) => extract(list.lift(index.toInt), tailSteps)
                case _ => {
                  throw new Exception(s"error: ${step} should be for list")
                }
              }
            }
            case COUNT_REGEX() => {
              data match {
                case Some(list: List[Any]) => Some(list.size)
                case Some(map: Map[String, Any]) => Some(map.size)
                case _ => {
                  throw new Exception(s"error: ${step} should be for list or map")
                }
              }
            }
            case _ => {
              throw new Exception(s"error: ${step} undefined")
            }
          }
        }
      }
    } match {
      case Success(res) => res
      case Failure(ex) => {
        println(s"extract failure: ${ex.getMessage}")
        None
      }
    }

  }

  def stepHead(path: List[String], step: String): List[String] = {
    path :+ step
  }

  def extractList(pathDatas: List[(List[String], Option[_])], steps: List[String]): List[(List[String], Option[_])] = {
    Try {
      steps match {
        case Nil => pathDatas
        case step :: tailSteps => {
          pathDatas.flatMap { pathData =>
            val (path, data) = pathData
            step match {
              case JSON_REGEX() => {
                data match {
//                  case Some(line: String) => extractList((stepHead(path, step), Some(JsonUtil.toMap[Any](line))) :: Nil, tailSteps)
                  case Some(line: String) => extractList((stepHead(path, step), Some(JsonUtil.toAnyMap(line))) :: Nil, tailSteps)
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for string")
                  }
                }
              }
              case MAP_REGEX(key) => {
                data match {
                  case Some(map: Map[String, Any]) => extractList((stepHead(path, step), map.get(key)) :: Nil, tailSteps)
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for map")
                  }
                }
              }
              case LIST_REGEX(index) => {
                data match {
                  case Some(list: List[Any]) => extractList((stepHead(path, step), list.lift(index.toInt)) :: Nil, tailSteps)
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for list")
                  }
                }
              }
              case ALL_LIST_REGEX() => {
                data match {
                  case Some(list: List[Any]) => {
                    val idxes = (0 to (list.size - 1)).toList
                    val newList = idxes.map { idx =>
                      val newStep = s"${step}_${idx}"
                      (stepHead(path, newStep), list.lift(idx))
                    }
                    extractList(newList, tailSteps)
                  }
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for list")
                  }
                }
              }
              case FILTER_LIST_REGEX(key, value) => {
                data match {
                  case Some(list: List[Any]) => {
                    val filteredList = list.filter { item =>
                      item match {
                        case mp: Map[String, Any] => {
                          mp.get(key) match {
                            case Some(v) => v == value
                            case _ => false
                          }
                        }
                        case _ => false
                      }
                    }
                    val idxes = (0 to (filteredList.size - 1)).toList
                    val newList = idxes.map { idx =>
                      val newStep = s"${step}_${idx}"
                      (stepHead(path, newStep), filteredList.lift(idx))
                    }
                    extractList(newList, tailSteps)
                  }
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for list")
                  }
                }
              }
              case COUNT_REGEX() => {
                data match {
                  case Some(list: List[Any]) => (stepHead(path, step), Some(list.size)) :: Nil
                  case Some(map: Map[String, Any]) => (stepHead(path, step), Some(map.size)) :: Nil
                  case _ => {
//                    extractList((stepHead(path, step), None) :: Nil, tailSteps)
                    throw new Exception(s"error: ${step} should be for list or map")
                  }
                }
              }
              case _ => {
//                extractList((stepHead(path, step), None) :: Nil, tailSteps)
                throw new Exception(s"error: ${step} undefined")
              }
            }
          }
        }
      }
    } match {
      case Success(res) => res
      case Failure(ex) => {
        println(s"extract failure: ${ex.getMessage}")
        Nil
      }
    }

  }

  def extractPathDataListWithSchemaMap(pathDatas: List[(List[String], Option[_])], stepsMap: Map[String, List[String]]): List[Map[String, Option[_]]] = {
    val schemaValues: Map[String, List[(List[String], Option[_])]] = stepsMap.mapValues { steps =>
      extractList(pathDatas, steps)
    }

    ValueListCombineUtil.cartesian(schemaValues)
  }

  def extractDataListWithSchemaMap(datas: List[Option[_]], stepsMap: Map[String, List[String]]): List[Map[String, Option[_]]] = {
    val pathDatas = datas.map((Nil, _))
    extractPathDataListWithSchemaMap(pathDatas, stepsMap)
  }

//  def extractDataListWithSchemaMapTest(datas: List[Option[_]], stepsMap: Map[String, List[String]]): Unit = {
//    val pathDatas = datas.map((Nil, _))
//
//    val schemaValues: Map[String, List[(List[String], Option[_])]] = stepsMap.mapValues { steps =>
//      extractList(pathDatas, steps)
//    }
//
//    ValueListCombineUtil.cartesianTest(schemaValues)
//  }

}
