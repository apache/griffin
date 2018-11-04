
package org.apache.griffin.measure.step.transform

import java.util.Date

import org.apache.griffin.measure.context.ContextId
import org.apache.griffin.measure.context.streaming.metric.CacheResults.CacheResult
import org.apache.griffin.measure.context.streaming.metric._
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.utils.ParamUtil._
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SQLContext, _}

import org.apache.spark.sql.functions.{lit, expr, col, rank}
import org.apache.spark.sql.DataFrame


object AnomalyDectectionOps {
  
  final val _anomalyCalc = "anomaly_calc"
  final val _anomalyMedian = "anomaly_median"
  
  def saveRecordsInCsv(spark: SQLContext) = DataFrame {
    val dfMarketDay = spark.sql("SELECT MARKET, LOCAL_DATE, sum(DURATION) as DURATION, sum(uplink+downlink) as PAYLOAD FROM LTE_MSISDN_LEAKAGE_DAILY GROUP BY MARKET, LOCAL_DATE")
    val dfMarketDayP2p = spark.sql("SELECT MARKET, LOCAL_DATE, P2P_PROTOCOL, sum(DURATION) as DURATION, sum(uplink+downlink) as PAYLOAD FROM LTE_MSISDN_LEAKAGE_DAILY GROUP BY MARKET, LOCAL_DATE, P2P_PROTOCOL")
    val dfDayP2p = spark.sql("SELECT LOCAL_DATE, P2P_PROTOCOL, sum(DURATION) as DURATION, sum(uplink+downlink) as PAYLOAD FROM LTE_MSISDN_LEAKAGE_DAILY GROUP BY LOCAL_DATE, P2P_PROTOCOL")
    val medianDurationMarketDay = getMedianValue(spark, "DURATION", dfMarketDay)
    val medianDurationMarketDayP2p = getMedianValue(spark, "DURATION", dfMarketDayP2p)
    val medianDurationDayP2p = getMedianValue(spark, "DURATION", dfDayP2p)
    println("Median Duration Value based on MARKET and Day is "+medianDurationMarketDay)
    println("Median Duration Value based on MARKET and Day and P2p is "+medianDurationMarketDayP2p)
    println("Median Duration Value based on Day and P2p is "+medianDurationDayP2p)
    val medianPayloadMarketDay = getMedianValue(spark, "PAYLOAD", dfMarketDay)
    val medianPayloadMarketDayP2p = getMedianValue(spark, "PAYLOAD", dfMarketDayP2p)
    val medianPayloadDayP2p = getMedianValue(spark, "PAYLOAD", dfDayP2p)
    println("Median Payload Value based on MARKET and Day is "+medianPayloadMarketDay)
    println("Median Payload Value based on MARKET and Day and P2p is "+medianPayloadMarketDayP2p)
    println("Median Payload Value based on Day and P2p is "+medianPayloadDayP2p)
    
    
    dfMarketDay.withColumn("MEDIAN DURATION",lit(medianDurationMarketDay)).withColumn("MEDIAN_PAYLOAD",lit(medianPayloadMarketDay))
    dfMarketDayP2p.withColumn("MEDIAN DURATION",lit(medianDurationMarketDayP2p)).withColumn("MEDIAN_PAYLOAD",lit(medianPayloadMarketDayP2p))
    dfDayP2p.withColumn("MEDIAN DURATION",lit(medianDurationDayP2p)).withColumn("MEDIAN_PAYLOAD",lit(medianPayloadDayP2p))
  }
  
  def getDuration(spark: SQLContext,inputTable:String) : DataFrame = {
    import spark.implicits._
    val lteDurationDaywiseDF = spark.sql(s"SELECT MARKET, LOCAL_DATE, sum(DURATION) as LTE_DURATION from $inputTable where RAT_TYPE = 6 GROUP BY MARKET, LOCAL_DATE ORDER BY LOCAL_DATE, MARKET")
    val nonLteDurationDaywiseDF = spark.sql(s"SELECT MARKET, LOCAL_DATE, sum(DURATION) as NON_LTE_DURATION from $inputTable where RAT_TYPE <> 6 GROUP BY MARKET, LOCAL_DATE ORDER BY LOCAL_DATE, MARKET")
    val avgLteDuration = lteDurationDaywiseDF.selectExpr("sum(LTE_DURATION)/count(1) as TOTAL_AVG_LTE_DURATION").map(_.getDouble(0)).take(1)(0)
    val avgNonLteDuration = nonLteDurationDaywiseDF.selectExpr("sum(NON_LTE_DURATION)/count(1) as TOTAL_AVG_NON_LTE_DURATION").map(_.getDouble(0)).take(1)(0)
    val daywiseCombinedDurationDF = lteDurationDaywiseDF.
      join(nonLteDurationDaywiseDF,Seq("MARKET", "LOCAL_DATE"),"full_outer").
      withColumn("TOTAL_AVG_LTE_DURATION",lit(avgLteDuration)).
      withColumn("TOTAL_AVG_NON_LTE_DURATION",lit(avgNonLteDuration)).
      withColumn("RATIO",$"NON_LTE_DURATION"/$"LTE_DURATION")
      daywiseCombinedDurationDF
  }
  
  //Returns DF where LTE Duration is less than nonLTE Duration
  def getAnamolityMarketData(spark: SQLContext, inputTable: String) : DataFrame = {
    val completeData = getDuration(spark, inputTable)
    val anamolousDF = completeData.where("RATIO >= 1")
    anamolousDF
  }
  
  //For each Market that has anomaly, get top <topP2PValues> based on orderBy<NON_LTE_DURATION/LTE_DURATION>
  def getAnamolityMarketTopP2PData(spark: SQLContext, inputTable: String, anamolousDF: DataFrame, orderBy: String, topP2PValues: Int) : DataFrame= {
    import spark.implicits._
    val orderByCol = "P2P_"+orderBy
    val df = spark.table(inputTable)
    val lteP2Pdf = df.where("rat_type = 6").
      groupBy("MARKET","P2P_PROTOCOL","LOCAL_DATE").
      agg(expr("sum(duration) as P2P_LTE_DURATION"))
    val nonLteP2Pdf = df.where("rat_type <> 6").
      groupBy("MARKET","P2P_PROTOCOL","LOCAL_DATE").
      agg(expr("sum(duration) as P2P_NON_LTE_DURATION"))
    val finalDF = lteP2Pdf.join(nonLteP2Pdf, Seq("MARKET","P2P_PROTOCOL","LOCAL_DATE"),"full_outer").
      withColumn("P2P_DURATION_RATIO",$"P2P_NON_LTE_DURATION"/$"P2P_LTE_DURATION")
    import org.apache.spark.sql.expressions.Window

    val totalAnamolousP2PData = anamolousDF.alias("A").join(finalDF.alias("B"), Seq("MARKET","LOCAL_DATE"),"left_outer").
    withColumn("RANK_NM",rank() over Window.partitionBy("MARKET","LOCAL_DATE").orderBy(col(orderByCol).desc))
    val topAnamolousP2PData = totalAnamolousP2PData.where("RANK_NM <= "+topP2PValues)
    topAnamolousP2PData
  }
  
  
  def operation(spark: SQLContext, lteOperation:Boolean, inputTable:String) : (Double, Double, Double, DataFrame, DataFrame) ={
    import spark.implicits._
    val whereCond = if(lteOperation) "=" else "<>"
    val testColumn = if(lteOperation) "LTE_DURATION" else "NON_LTE_DURATION"
    val df = spark.sql(s"SELECT MARKET, LOCAL_DATE, sum(DURATION) as $testColumn FROM $inputTable WHERE RAT_TYPE $whereCond 6 GROUP BY MARKET, LOCAL_DATE")
    val medianValueOriginal = getMedianValue(spark, testColumn,df)
    val newMedianValueDF2 = getDifferenceMedianDF(spark, testColumn,df,medianValueOriginal)
    val newMedianValue = getMedianValue(spark, testColumn,newMedianValueDF2)
    //----------------------------------------------------------------------------------//
    
    val mad = newMedianValue * 1.4826 //Median Absolute Deviation
    val upperLimit = medianValueOriginal + 2*mad
    val lowerLimit = medianValueOriginal - 2*mad
    val lowerLimitDetectedTempDf = df.where(s"${testColumn} < ${lowerLimit}")
    val upperLimitDetectedTempDf = df.where(s"${testColumn} > ${upperLimit}")
    val (lowerLimitDetectedDf,upperLimitDetectedDf) = 
      if(lteOperation){
        val nonLTEdf = spark.sql(s"SELECT MARKET, LOCAL_DATE, DURATION FROM $inputTable WHERE RAT_TYPE <> 6")
       ( if(lowerLimitDetectedTempDf.take(1).nonEmpty){
           lowerLimitDetectedTempDf.alias("A").
             join(nonLTEdf.alias("B"),Seq("MARKET","LOCAL_DATE"),"left_Outer").
             groupBy("MARKET","LOCAL_DATE",testColumn).agg(expr("sum(DURATION)").as("NON_LTE_DURATION")).
             withColumn("MEDIAN_VALUE",lit(medianValueOriginal)).
             withColumn("UPPER_LIMIT",lit(upperLimit)).
             withColumn("LOWER_LIMIT",lit(lowerLimit)).
             withColumn("RATIO",$"NON_LTE_DURATION"/$"LTE_DURATION")
         }else lowerLimitDetectedTempDf
       ,
        if(upperLimitDetectedTempDf.take(1).nonEmpty){
          upperLimitDetectedTempDf.alias("A").
            join(nonLTEdf.alias("B"),Seq("MARKET","LOCAL_DATE"),"left_Outer").
            groupBy("MARKET","LOCAL_DATE",testColumn).agg(expr("sum(DURATION)").as("NON_LTE_DURATION")).
            withColumn("MEDIAN_VALUE",lit(medianValueOriginal)).
            withColumn("UPPER_LIMIT",lit(upperLimit)).
            withColumn("LOWER_LIMIT",lit(lowerLimit)).
            withColumn("RATIO",$"NON_LTE_DURATION"/$"LTE_DURATION")
        }else upperLimitDetectedTempDf
       )
      }
    
      else {
        val ltedf = spark.sql(s"SELECT MARKET, LOCAL_DATE, DURATION FROM $inputTable WHERE RAT_TYPE = 6")
        ( if(lowerLimitDetectedTempDf.take(1).nonEmpty){
           lowerLimitDetectedTempDf.alias("A").
             join(ltedf.alias("B"),Seq("MARKET","LOCAL_DATE"),"left_Outer").
             groupBy("MARKET","LOCAL_DATE",testColumn).agg(expr("sum(DURATION)").as("LTE_DURATION")).
             select("MARKET","LOCAL_DATE","LTE_DURATION","NON_LTE_DURATION").
             withColumn("MEDIAN_VALUE",lit(medianValueOriginal)).
             withColumn("UPPER_LIMIT",lit(upperLimit)).
             withColumn("LOWER_LIMIT",lit(lowerLimit)).
             withColumn("RATIO",$"NON_LTE_DURATION"/$"LTE_DURATION")
         }else lowerLimitDetectedTempDf
       ,
        if(upperLimitDetectedTempDf.take(1).nonEmpty){
          upperLimitDetectedTempDf.alias("A").
            join(ltedf.alias("B"),Seq("MARKET","LOCAL_DATE"),"left_Outer").
            groupBy("MARKET","LOCAL_DATE",testColumn).agg(expr("sum(DURATION)").as("LTE_DURATION")).
            select("MARKET","LOCAL_DATE","LTE_DURATION","NON_LTE_DURATION").
            withColumn("MEDIAN_VALUE",lit(medianValueOriginal)).
            withColumn("UPPER_LIMIT",lit(upperLimit)).
            withColumn("LOWER_LIMIT",lit(lowerLimit)).
            withColumn("RATIO",$"NON_LTE_DURATION"/$"LTE_DURATION")
        }else upperLimitDetectedTempDf
       )
        }
    (medianValueOriginal,upperLimit,lowerLimit,lowerLimitDetectedDf,upperLimitDetectedDf)
  }
  
   def getDifferenceMedianDF(spark: SQLContext, inputColumn: String, inputDF: DataFrame, medianValue: Double ) : DataFrame = {
    import spark.implicits._
    inputDF.selectExpr("MARKET","LOCAL_DATE",s"abs(${inputColumn} - ${medianValue}) as "+inputColumn)
  }
  
  def getMedianValue(spark: SQLContext, inputColumn: String, inputDF: DataFrame) : Double = {
    import spark.implicits._
    val medianIndex = inputDF.count/2
    val flagMedianIndexEven = if(medianIndex.isValidLong) true else false
    if(flagMedianIndexEven) {
      val medianValuesArray = inputDF.selectExpr(s"${inputColumn}", s"row_number() over (order by ${inputColumn}) as R_NUM").
        where(s"R_NUM IN ( ${medianIndex} , ${medianIndex} + 1) ").map(_.getDouble(0)).take(2)
      medianValuesArray.sum/2
    }else{
      val medianValue = inputDF.selectExpr(s"${inputColumn}", s"row_number() over (order by ${inputColumn}) as R_NUM").
        where(s"R_NUM = ( ${medianIndex} + 0.5) ").map(_.getDouble(0)).first
      medianValue
    }
  }
  
  //Case 1
  def anomalyDetectionComparison(spark: SQLContext): DataFrame = {
    val totalMarketWiseAnomalyDF=getAnamolityMarketData(spark, "DEMO_LTE")
    val totalMarketP2pWiseAnomalyDF = getAnamolityMarketTopP2PData(spark, "DEMO_LTE",totalMarketWiseAnomalyDF,"NON_LTE_DURATION",5).withColumn("ERROR_ID",lit(1))// 2 Protocol wise Anomality Data
    totalMarketP2pWiseAnomalyDF
  }
  
  def anomalyDetectionBasedOnRange(spark: SQLContext): DataFrame = {
    val (medianLTE,upperLTE,lowerLTE,lowerLTEDF,upperLTEDF) = operation(spark, true,"DEMO_LTE")//LTE
    val (medianNonLTE,upperNonLTE,lowerNonLTE,lowerNonLTEDF,upperNonLTEDF)= operation(spark, false,"DEMO_LTE")//NON_LTE
    
    val topP2PUpperLteBasedOnTopLteDuration = getAnamolityMarketTopP2PData(spark, "DEMO_LTE",upperLTEDF,"LTE_DURATION",5).withColumn("ERROR_ID",lit(2)) // 7 P2P protocal wise Data with LTE Duration greater than the upper limit. top 5 Protocol based on LTE_Duration
    val topP2PLowerLteBasedOnTopLteDuration = lowerLTEDF//9
    val topP2PUpperNonLteBasedOnTopNonLteDuration = getAnamolityMarketTopP2PData(spark, "DEMO_LTE",upperNonLTEDF,"NON_LTE_DURATION",5).withColumn("ERROR_ID",lit(3))// 12 P2P protocal wise Data with Non LTE Duration greater than the upper limit. top 5 Protocol based on Non LTE_Duration
    val topP2PLowerNonLteBasedOnTopNonLteDuration = lowerNonLTEDF// 13
    
    val returnDf = topP2PUpperLteBasedOnTopLteDuration.union(topP2PUpperNonLteBasedOnTopNonLteDuration)
    returnDf
  }
  
}
