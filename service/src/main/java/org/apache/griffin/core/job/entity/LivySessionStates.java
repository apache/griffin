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

package org.apache.griffin.core.job.entity;

import com.cloudera.livy.sessions.SessionState;

public class LivySessionStates {

    /**
     * unknown is used to represent the state that server get null from Livy.
     * the other state is just same as com.cloudera.livy.sessions.SessionState.
     */
    public enum State {
        not_started,
        starting,
        recovering,
        idle,
        running,
        busy,
        shutting_down,
        error,
        dead,
        success,
        unknown
    }

    public static SessionState toSessionState(State state) {
        switch (state) {
            case not_started:
                return new SessionState.NotStarted();
            case starting:
                return new SessionState.Starting();
            case recovering:
                return new SessionState.Recovering();
            case idle:
                return new SessionState.Idle();
            case running:
                return new SessionState.Running();
            case busy:
                return new SessionState.Busy();
            case shutting_down:
                return new SessionState.ShuttingDown();
            case error:
                return new SessionState.Error(System.nanoTime());
            case dead:
                return new SessionState.Dead(System.nanoTime());
            case success:
                return new SessionState.Success(System.nanoTime());
            default:
                return null;
        }
    }

    public static boolean isActive(State state) {
        if (State.unknown.equals(state)) {
            // set unknown isactive() as false.
            return false;
        }
        SessionState sessionState = toSessionState(state);
        if (sessionState == null) {
            return false;
        } else {
            return sessionState.isActive();
        }
    }

    public static boolean isHealthy(State state) {
        if (State.error.equals(state) || State.dead.equals(state) || State.shutting_down.equals(state)) {
            return false;
        }
        return true;
    }
}
