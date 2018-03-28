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

package org.apache.griffin.core.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.lang.reflect.Method;

public class TokenInterceptor extends HandlerInterceptorAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenInterceptor.class);
    private static final String TOKEN = "token";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (handler instanceof HandlerMethod) {
            Method method = ((HandlerMethod) handler).getMethod();
            Token annotation = method.getAnnotation(Token.class);
            if (annotation != null) {
                LOGGER.info("enter interceptor");
                if (isRepeatSubmit(request)) {
                    LOGGER.warn("Please don't repeat submit url {}.", request.getServletPath());
                    return false;
                } else {
                    LOGGER.info("not repeat submit");
                }
                return true;
            }
            return true;
        } else {
            return super.preHandle(request, response, handler);
        }

    }

    private boolean isRepeatSubmit(HttpServletRequest request) {
//        String curToken = request.getHeader(TOKEN);
//        HttpSession session = request.getSession(true);
//        Object preToken = session.getAttribute(TOKEN);
//        //if http header has no token,we ignore to deal with repeated submission.
//        if (curToken == null) {
//            return false;
//        } else if (preToken == null) {
//            session.setAttribute(TOKEN, curToken);
//            return false;
//        } else {
//            if (preToken.toString().equals(curToken)) {
//                return true;
//            } else {
//                session.setAttribute(TOKEN, curToken);
//                return false;
//            }
//        }
        return false;
    }
}
