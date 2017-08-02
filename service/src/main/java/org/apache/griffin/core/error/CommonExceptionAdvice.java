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
*//*

package org.apache.griffin.core.error;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.griffin.core.error.Exception.GetHealthInfoFailureException;
import org.apache.griffin.core.error.Exception.GetJobsFailureException;
import org.apache.griffin.core.error.Exception.HiveConnectionException;
import org.apache.griffin.core.error.Exception.KafkaConnectionException;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.web.DefaultErrorAttributes;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

*/
/**
 * Created by xiangrchen on 7/24/17.
 *//*

@ControllerAdvice
@ResponseBody
public class CommonExceptionAdvice extends ResponseEntityExceptionHandler{

  private static final Logger log = LoggerFactory.getLogger(CommonExceptionAdvice.class);

  */
/**
   * Hive Server Error
   *//*

  @ExceptionHandler(HiveConnectionException.class)
  public Map<String, Object> handleHiveConnectionException(HiveConnectionException e,
      HttpServletRequest request) {
    return setExceptionResponse(request, HttpStatus.GATEWAY_TIMEOUT,
        GriffinOperationMessage.HIVE_CONNECT_FAILED);
    //return new ResponseEntity<>(, )
  }


  */
/**
   * Kafka Server Error
   *//*

  @ExceptionHandler(KafkaConnectionException.class)
  public Map<String, Object> handleKafkaConnectionException(KafkaConnectionException e,
      HttpServletRequest request) {
    Map<String, Object> map = setExceptionResponse(request, HttpStatus.GATEWAY_TIMEOUT,
        GriffinOperationMessage.KAFKA_CONNECT_FAILED);
    return map;
  }


  @ExceptionHandler(GetHealthInfoFailureException.class)
  public Map<String, Object> handleGetHealthInfoFailureException(
      GetHealthInfoFailureException e, HttpServletRequest request) {
    return setExceptionResponse(request, HttpStatus.INTERNAL_SERVER_ERROR,
        GriffinOperationMessage.GET_HEALTHINFO_FAIL);
  }

  @ExceptionHandler(GetJobsFailureException.class)
  public Map<String, Object> handleGetJobsFailureException(
      GetJobsFailureException e, HttpServletRequest request) {
    return setExceptionResponse(request, HttpStatus.INTERNAL_SERVER_ERROR,
        GriffinOperationMessage.Get_JOBS_FAIL);

  }

  private Map<String, Object> setExceptionResponse(HttpServletRequest request, HttpStatus status,
      GriffinOperationMessage message) {
    request.setAttribute("javax.servlet.error.status_code", status.value());
    request.setAttribute("javax.servlet.error.message", message.getDescription());
    request.setAttribute("javax.servlet.error.error", status.toString());
    return (new DefaultErrorAttributes())
        .getErrorAttributes(new ServletWebRequest(request), false);
  }

}*/
