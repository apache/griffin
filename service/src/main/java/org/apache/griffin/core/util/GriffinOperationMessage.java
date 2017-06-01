/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.util;


public enum GriffinOperationMessage {
    RESOURCE_NOT_FOUND(-1, "Resource Not Found"),
    DELETE_MEASURE_BY_NAME_SUCCESS(0, "Delete Measures By Name Succeed"),
    DELETE_MEASURE_BY_NAME_FAIL(1, "Delete Measures By Name Failed"),
    UPDATE_MEASURE_SUCCESS(2, "Update Measure Succeed"),
    UPDATE_MEASURE_FAIL(3, "Update Measure Failed"),
    CREATE_MEASURE_SUCCESS(4, "Create Measure Succeed"),
    CREATE_MEASURE_FAIL(5, "Create Measure Failed"),
    CREATE_MEASURE_FAIL_DUPLICATE(6, "Create Measure Failed, duplicate records"),
    ;

    private final int code;
    private final String description;

    private GriffinOperationMessage(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code + ": " + description;
    }
}
