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
    if (args.length < 2) {
      error("Usage: class <env-param-file> <user-param-file> [file-system-of-param-file: local or hdfs(default)]")
      sys.exit(-1)
    }

    val envParamFile = args(0)
    val userParamFile = args(1)
    val fsType = if (args.length > 2) args(2) else "hdfs"

    // read param files
    val envParam = readParamFile[EnvParam](envParamFile, fsType) match {
      case Success(p) => p
      case Failure(ex) => {
        error(ex.getMessage)
        sys.exit(-2)
      }
    }
    val userParam = readParamFile[UserParam](userParamFile, fsType) match {
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
