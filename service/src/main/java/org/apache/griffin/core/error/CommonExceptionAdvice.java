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
package org.apache.griffin.core.error;

import org.apache.griffin.core.error.Exception.HiveConnectionException;
import org.apache.griffin.core.error.Exception.KafkaConnectionException;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by xiangrchen on 7/24/17.
 */
@ControllerAdvice
@ResponseBody
public class CommonExceptionAdvice {
    private static final Logger log = LoggerFactory.getLogger(CommonExceptionAdvice.class);

    /**
     * Hive Server Error
     */
    @ExceptionHandler(HiveConnectionException.class)
    public ResponseEntity<String>  handleHiveConnectionException(Exception e){
        log.error(GriffinOperationMessage.HIVE_CONNECT_FAILED+""+e);
        ResponseEntity<String> responseEntity=new ResponseEntity<String>(GriffinOperationMessage.HIVE_CONNECT_FAILED.toString(), HttpStatus.GATEWAY_TIMEOUT);
        return responseEntity;
    }

    /**
     * Kafka Server Error
     */
    @ExceptionHandler(KafkaConnectionException.class)
    public ResponseEntity<String>  handleKafkaConnectionException(Exception e){
        log.error(GriffinOperationMessage.KAFKA_CONNECT_FAILED+""+e);
        ResponseEntity<String> responseEntity=new ResponseEntity<String>(GriffinOperationMessage.KAFKA_CONNECT_FAILED.toString(), HttpStatus.GATEWAY_TIMEOUT);
        return responseEntity;
    }

}
