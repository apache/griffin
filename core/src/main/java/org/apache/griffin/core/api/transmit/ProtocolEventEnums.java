package org.apache.griffin.core.api.transmit;

public enum ProtocolEventEnums {
    TASK_ASSIGN_REQUEST(1, "the request for tm to assign task to en"),
    TASK_ASSIGN_RESPONSE(2, "the response for en to reply to tm");

    private int type;
    private String desc;

    ProtocolEventEnums(int type, String desc) {
        this.type = type;
        this.desc = desc;
    }
}
