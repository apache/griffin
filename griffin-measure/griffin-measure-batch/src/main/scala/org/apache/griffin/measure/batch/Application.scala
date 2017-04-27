package org.apache.griffin.measure.batch

import org.apache.griffin.measure.batch.algo.{Algo, BatchAccuracyAlgo}
import org.apache.griffin.measure.batch.config.params._
import org.apache.griffin.measure.batch.config.params.env._
import org.apache.griffin.measure.batch.config.params.user._
import org.apache.griffin.measure.batch.config.reader._
import org.apache.griffin.measure.batch.config.validator.AllParamValidator
import org.apache.griffin.measure.batch.log.Loggable

import scala.util.{Failure, Success, Try}

object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    info(args.toString)
    if (args.length < 2) {
      error("Usage: class <env-param> <user-param> [List of String split by comma: raw | local | hdfs(default)]")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val userParamFile = args(1)
    val (envFsType, userFsType) = if (args.length > 2) {
      val fsTypes = args(2).trim.split(",")
      if (fsTypes.length == 1) (fsTypes(0).trim, fsTypes(0).trim)
      else if (fsTypes.length >= 2) (fsTypes(0).trim, fsTypes(1).trim)
      else ("hdfs", "hdfs")
    } else ("hdfs", "hdfs")

    info(envParamFile)
    info(userParamFile)

    // read param files
    val envParam = readParamFile[EnvParam](envParamFile, envFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val userParam = readParamFile[UserParam](userParamFile, userFsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val allParam: AllParam = AllParam(envParam, userParam)

    // validate param files
    validateParams(allParam) match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-3)
      }
      case _ => {
        info("params validation pass")
      }
    }

    // choose algorithm   // fixme: not done, need to choose algorithm by param
    val dqType = allParam.userParam.dqType
    val algo: Algo = BatchAccuracyAlgo(allParam)

    algo.run match {
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-4)
      }
      case _ => {
        info("calculation finished")
      }
    }
  }

  private def readParamFile[T <: Param](file: String, fsType: String)(implicit m : Manifest[T]): Try[T] = {
    val paramReader = ParamReaderFactory.getParamReader(file, fsType)
    paramReader.readConfig[T]
  }

  private def validateParams(allParam: AllParam): Try[Boolean] = {
    val allParamValidator = AllParamValidator()
    allParamValidator.validate(allParam)
  }

}
