package org.apache.griffin.validility

import org.apache.griffin.dataLoaderUtils.DataLoaderFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.griffin.util.{DataConverter, DataTypeUtils, HdfsUtils, PartitionUtils}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object Vali extends Logging {

  def main(args: Array[String]): Unit ={
    if (args.length < 2) {
      logError("Usage: class <input-conf-file> <outputPath>")
      logError("For input-conf-file, please use vali_config.json as an template to reflect test dataset accordingly.")
      sys.exit(-1)
    }
    val input = HdfsUtils.openFile(args(0))

    val outputPath = args(1) + System.getProperty("file.separator")

    //add files for job scheduling
    val startFile = outputPath + "_START"
    val resultFile = outputPath + "_RESULT"
    val doneFile = outputPath + "_FINISHED"

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    //read the config info of comparison
    val configure = mapper.readValue(input, classOf[ValidityConfEntity])

    val conf = new SparkConf().setAppName("Vali")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //add spark applicationId for debugging
    val applicationId = sc.applicationId

    //for spark monitoring
    HdfsUtils.writeFile(startFile, applicationId)

    //get data
    val dataLoader = DataLoaderFactory.getDataLoader(sqlContext, DataLoaderFactory.hive)
    val sojdf = dataLoader.getValiDataFrame(configure)

    //-- algorithm --
    calcVali(configure, sojdf)

    //--output metrics data--
    val out = HdfsUtils.createFile(resultFile)
    mapper.writeValue(out, configure)

    //for spark monitoring
    HdfsUtils.createFile(doneFile)

    sc.stop()
  }

  def calcVali(configure: ValidityConfEntity, sojdf: DataFrame) : Unit = {
    val dfCount = sojdf.count()

    //--1. get all cols name, and types--
    val fnts = sojdf.schema.fields.map(x => (x.name, x.dataType.simpleString)).toMap

    //get col type
    val req: List[ValidityReq] = configure.validityReq.map { r =>
      val fv = fnts.getOrElse(r.colName, None)
      if (fv != None) {
        r.colType = fv.toString
        r.isNum = DataTypeUtils.isNum(r.colType)
      }
      r
    }

    //--2. calc num cols metrics--
    val numcols = req.filter(r => r.isNum)

    val numIdx = numcols.map(c => c.colId).toArray
    val numIdxZip = numIdx.zipWithIndex
    val numColsCount = numcols.length

    //median number function
    def funcMedian(df: DataFrame, col: Int): Double = {
      val dt = sojdf.schema(col).dataType
      val getFunc = DataTypeUtils.dataType2RowGetFunc(dt)

      val mp = df.map { v =>
        if (v.isNullAt(col)) (0.0, 0L)
        else (DataConverter.getDouble(getFunc(v, col)), 1L)
      }.reduceByKey(_+_)
      val allCnt = mp.aggregate(0L)((c, m) => c + m._2, _+_)
      val cnt = mp.sortByKey().collect()
      var tmp, tmp1 = 0L
      var median, median1 = cnt(0)._1
      if (allCnt % 2 != 0) {
        for (i <- 0 until cnt.length if (tmp < allCnt / 2 + 1)) {
          tmp += cnt(i)._2
          median = cnt(i)._1
        }
        median
      } else {
        for (i <- 0 until cnt.length if (tmp1 < allCnt / 2 + 1)) {
          tmp1 += cnt(i)._2
          median1 = cnt(i)._1
          if (tmp < allCnt / 2) {
            tmp = tmp1
            median = median1
          }
        }
        (median + median1) / 2
      }
    }

    //match num metrics request
    def getNumStats(smry: MultivariateStatisticalSummary, df: DataFrame, op: Int, col: Int): Any = {
      val i = numIdx.indexWhere(_ == col)
      if (i >= 0) {
        MetricsType(op) match {
          case MetricsType.TotalCount => smry.count
          case MetricsType.Maximum => smry.max(i)
          case MetricsType.Minimum => smry.min(i)
          case MetricsType.Mean => smry.mean(i)
          case MetricsType.Median => funcMedian(df, col)
//          case MetricsType.Variance => smry.variance(i)
//          case MetricsType.NumNonZeros => smry.numNonzeros(i)
          case _ => None
        }
      }
    }

    if (numColsCount > 0) {
      val idxType = numIdxZip.map(i => (i._2, i._1, sojdf.schema(i._1).dataType))

      //calc metrics of all numeric cols once
      val numcolVals = sojdf.map { row =>
        val vals = idxType.foldLeft((List[Int](), List[Double]())) { (arr, i) =>
          if (row.isNullAt(i._2)) arr
          else {
            val v = DataTypeUtils.dataType2RowGetFunc(i._3)(row, i._2)
            (i._1 :: arr._1, DataConverter.getDouble(v) :: arr._2)
          }
        }
        Vectors.sparse(numColsCount, vals._1.toArray, vals._2.toArray)
      }

      val summary = Statistics.colStats(numcolVals)

      //get numeric metrics from summary
      numcols.foreach(vr => vr.metrics.foreach(mc => mc.result = getNumStats(summary, sojdf, mc.name, vr.colId)))
    }

    //--3. calc str/other cols metrics--
    val strcols = req.filter(r => !r.isNum)

    //count function
    def funcCount(df: DataFrame, col: Int): Long = {
      dfCount
    }
    //null count function
    def funcNullCount(df: DataFrame, col: Int): Long = {
      val nullRow = df.map(row => if (row.isNullAt(col)) 1L else 0)
      nullRow.fold(0)((a,b)=>a+b)
    }
    //unique count function
    def funcUniqueCount(df: DataFrame, col: Int): Long = {
      val dt = sojdf.schema(col).dataType
      val getFunc = DataTypeUtils.dataType2RowGetFunc(dt)

      val mp = df.map(v=>(DataConverter.getString(getFunc(v, col))->1L))
      val rs = mp.reduceByKey(_+_)
      rs.count()
    }
    //duplicate count function
    def funcDuplicateCount(df: DataFrame, col: Int): Long = {
      val dt = sojdf.schema(col).dataType
      val getFunc = DataTypeUtils.dataType2RowGetFunc(dt)

      val mp = df.map(v=>(DataConverter.getString(getFunc(v, col))->1L))
      val rs = mp.reduceByKey(_+_)
      rs.aggregate(0)((s, v) => if (v._2 == 1) s else s + 1, (s1, s2) => s1 + s2)
    }

    //regex and match str metrics request
    def getStrResult(df: DataFrame, op: Int, col: Int): Any = {
      MetricsType(op) match {
        case MetricsType.TotalCount => funcCount(df, col)
        case MetricsType.NullCount => funcNullCount(df, col)
        case MetricsType.UniqueCount => funcUniqueCount(df, col)
        case MetricsType.DuplicateCount => funcDuplicateCount(df, col)
        case _ => None
      }
    }

    if (strcols.length > 0) {
      //calc str metrics one by one
      strcols.foreach(vr => vr.metrics.foreach(mc => mc.result = getStrResult(sojdf, mc.name, vr.colId)))
    }

    //union the num cols and str cols metrics, and put the result into configure object
    val rsltCols = numcols.union(strcols)
    configure.validityReq = rsltCols

    //output: need to change
    logInfo("== result ==\n" + rsltCols)
  }

}