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
package org.apache.griffin.measure.log

import org.slf4j.LoggerFactory

trait Loggable {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

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
