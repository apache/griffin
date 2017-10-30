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

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

public abstract class GriffinException extends RuntimeException {
    @ResponseStatus(value = HttpStatus.GATEWAY_TIMEOUT, reason = "Fail to Connect Kafka")
    public static class KafkaConnectionException extends GriffinException {
    }

    @ResponseStatus(value = HttpStatus.GATEWAY_TIMEOUT, reason = "Fail to Connect Hive")
    public static class HiveConnectionException extends GriffinException {
    }

    @ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Fail to Get HealthInfo")
    public static class GetHealthInfoFailureException extends GriffinException {
    }

    @ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "Fail to Get Jobs")
    public static class GetJobsFailureException extends GriffinException {
    }
}
