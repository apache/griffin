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

package org.apache.griffin.core.login;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class LoginServiceDefaultImpl implements LoginService {

    @Override
    public ResponseEntity<Map<String, Object>> login(Map<String, String> map) {
        String username = map.get("username");
        if (StringUtils.isBlank(username)) {
            username = "Anonymous";
        }
        String fullName = username;
        Map<String, Object> message = new HashMap<>();
        message.put("ntAccount", username);
        message.put("fullName", fullName);
        message.put("status", 0);
        return new ResponseEntity<>(message, HttpStatus.OK);
    }
}
