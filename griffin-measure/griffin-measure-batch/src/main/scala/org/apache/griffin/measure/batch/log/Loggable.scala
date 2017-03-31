package org.apache.griffin.measure.batch.log

import org.slf4j.LoggerFactory

trait Loggable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  protected def info(msg: String): Unit = {
    logger.info(msg)
  }

}
