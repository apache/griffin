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

package org.apache.griffin.core.util;


import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum GriffinOperationMessage {
    //success
    CREATE_MEASURE_SUCCESS(201, "Create Measure Succeed"),
    DELETE_MEASURE_BY_ID_SUCCESS(202, "Delete Measures By Name Succeed"),
    DELETE_MEASURE_BY_NAME_SUCCESS(203, "Delete Measures By Name Succeed"),
    UPDATE_MEASURE_SUCCESS(204, "Update Measure Succeed"),
    CREATE_JOB_SUCCESS(205, "CREATE Job Succeed"),
    DELETE_JOB_SUCCESS(206, "Delete Job Succeed"),
    SET_JOB_DELETED_STATUS_SUCCESS(207, "Set Job Deleted Status Succeed"),
    PAUSE_JOB_SUCCESS(208, "Pause Job Succeed"),
    UPDATE_JOB_INSTANCE_SUCCESS(209, "Update Job Instance Succeed"),

    //failed
    RESOURCE_NOT_FOUND(400, "Resource Not Found"),
    CREATE_MEASURE_FAIL(401, "Create Measure Failed"),
    DELETE_MEASURE_BY_ID_FAIL(402, "Delete Measures By Name Failed"),
    DELETE_MEASURE_BY_NAME_FAIL(403, "Delete Measures By Name Failed"),
    UPDATE_MEASURE_FAIL(404, "Update Measure Failed"),
    CREATE_JOB_FAIL(405, "Create Job Failed"),
    DELETE_JOB_FAIL(406, "Delete Job Failed"),
    SET_JOB_DELETED_STATUS_FAIL(407, "Set Job Deleted Status Failed"),
    PAUSE_JOB_FAIL(408, "Pause Job Failed"),
    UPDATE_JOB_INSTANCE_FAIL(409, "Update Job Instance Failed"),
    CREATE_MEASURE_FAIL_DUPLICATE(410, "Create Measure Failed, duplicate records"),
    UNEXPECTED_RUNTIME_EXCEPTION(411, "Unexpected RuntimeException");

    private final int code;
    private final String description;

    GriffinOperationMessage(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "GriffinOperationMessage{" +
                "code=" + code +
                ", description='" + description + '\'' +
                '}';
    }
}
