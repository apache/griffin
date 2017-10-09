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

package org.apache.griffin.core.error.exception;

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

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@ControllerAdvice
@ResponseBody
public class RuntimeExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeExceptionHandler.class);

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleUnexpectedRuntimeException(RuntimeException e, HttpServletRequest request) {
        LOGGER.error("Unexpected RuntimeException. " + e);
        return setExceptionResponse(request, HttpStatus.INTERNAL_SERVER_ERROR, GriffinOperationMessage.UNEXPECTED_RUNTIME_EXCEPTION);
    }

    @ExceptionHandler(value = GriffinException.class)
    public void handleCustomException(GriffinException e) throws GriffinException {
        throw e;
    }

    private ResponseEntity<Map<String, Object>> setExceptionResponse(HttpServletRequest request, HttpStatus status,
                                                                     GriffinOperationMessage message) {
        request.setAttribute("javax.servlet.error.status_code", status.value());
        request.setAttribute("javax.servlet.error.message", message.getDescription());
        request.setAttribute("javax.servlet.error.error", status.toString());
        request.setAttribute("javax.servlet.error.request_uri", request.getRequestURI());
        Map<String, Object> map = (new DefaultErrorAttributes())
                .getErrorAttributes(new ServletWebRequest(request), false);
        return new ResponseEntity(map, status);
    }
}
