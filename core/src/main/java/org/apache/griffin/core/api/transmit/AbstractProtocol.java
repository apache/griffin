package org.apache.griffin.core.api.transmit;

public abstract class AbstractProtocol implements GriffinProtocolEvent{
    private transient String ip;
    private transient int port;
    public abstract ProtocolEventEnums getProtocolEvent();
}
