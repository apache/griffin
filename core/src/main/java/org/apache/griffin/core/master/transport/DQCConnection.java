package org.apache.griffin.core.master.transport;

import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.Future;

/**
 * the obj has a socketChannel to worker node
 */
public class DQCConnection {
    // worker hostName
    private String hostName;
    // worker hostIP
    private String hostIP;
    // worker hostPort
    private int hostPort;
    // todo
    private ServerSocketChannel channel;

    /**
     * Send msg async
     * @param msg message
     * @return Future
     */
    public Future send(byte[] msg) {
        return null;
    }
}
