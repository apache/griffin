//package org.apache.griffin.record.cleaner
//
//import com.ebay.griffin.config.params.{AppParam, DataParam}
//
//class CleanerFactory(appParam: AppParam, dataParam: DataParam) {
//
//  var cleaner: HdfsCleaner = null
//
//  def cleanerStart(): Unit = {
//    val recorderType = appParam.getParam(appParam.RecorderType).toString
//    if (cleaner == null) {
//      cleaner = recorderType match {
//        case appParam.RecorderHdfs => {
//          new HdfsCleaner(appParam, dataParam)
//        }
//        case appParam.RecorderPost => {
//          new HdfsCleaner(appParam, dataParam)
//        }
//        case _ => null
//      }
//      cleaner.start
//    }
//  }
//
//  def cleanerEnd(): Unit = {
//    if (cleaner != null) {
//      cleaner.end
//    }
//  }
//
//}
