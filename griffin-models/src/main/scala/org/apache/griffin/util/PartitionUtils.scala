package org.apache.griffin.util

import org.apache.griffin.common.PartitionPair

object PartitionUtils {
  def generateWhereClause(partition: List[PartitionPair]): String = {
    var first = true
    partition.foldLeft("") { (clause, pair) =>
      if (first) {
        first = false
        s"where ${pair.colName} = ${pair.colValue}"
      }
      else s"$clause AND ${pair.colName} = ${pair.colValue}"
    }
  }

  def generateSourceSQLClause(sourceTable: String, partition: List[PartitionPair]): String = {
    s"SELECT * FROM $sourceTable ${generateWhereClause(partition)}"
  }

  def generateTargetSQLClause(targetTable: String, partitions: List[List[PartitionPair]]): String = {
    var first = true
    partitions.foldLeft(s"SELECT * FROM $targetTable") { (clause, partition) =>
      if (first) {
        first = false
        s"$clause ${generateWhereClause(partition)}"
      }
      else s"$clause UNION ALL SELECT * FROM $targetTable ${generateWhereClause(partition)}"
    }
  }
}