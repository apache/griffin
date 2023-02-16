package org.apache.griffin.api.transmit;

public abstract class AbstractProtocol implements GriffinProtocolEvent{
    private transient String ip;
    private transient int port;
    public abstract ProtocolEventEnums getProtocolEvent();
}
