package org.apache.griffin.core.enums;


import com.google.common.collect.ImmutableList;
import org.apache.griffin.common.util.Enums;

import java.util.List;
import java.util.Map;
import java.util.Objects;


public enum RunState {

    WAITING(10, false, false, "TO RUN"),
    RUNNING(20, false, false, "RUNNING"),
    PAUSED(30, false, false, "PAUSED"),
    FINISHED(40, true, false, "FINISHED"),
    CANCELED(50, true, true, "已取消"),
    ;

    private final int value;
    private final boolean terminal;
    private final boolean failure;
    private final String desc;

    RunState(int value, boolean terminal, boolean failure, String desc) {
        this.value = value;
        this.terminal = terminal;
        this.failure = failure;
        this.desc = desc;
    }

    public int value() {
        return value;
    }
    public String desc() {
        return desc;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isFailure() {
        return failure;
    }

    public static RunState of(Integer value) {
        return Objects.requireNonNull(Const.MAPPING.get(value), () -> "Invalid run state value: " + value);
    }

    public static final class Const {
        private static final Map<Integer, RunState> MAPPING = Enums.toMap(RunState.class, RunState::value);

        public static final List<RunState> PAUSABLE_LIST = ImmutableList.of(WAITING, RUNNING);
        public static final List<RunState> RUNNABLE_LIST = ImmutableList.of(WAITING, PAUSED);
        public static final List<RunState> TERMINABLE_LIST = ImmutableList.of(WAITING, RUNNING, PAUSED);
    }

}
