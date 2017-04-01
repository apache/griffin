package org.apache.griffin.measure.batch.log

import org.slf4j.LoggerFactory

trait Loggable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  protected def info(msg: String): Unit = {
    logger.info(msg)
  }

  protected def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  protected def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  protected def error(msg: String): Unit = {
    logger.error(msg)
  }

}
