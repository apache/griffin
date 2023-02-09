package org.apache.griffin.core.master.transport;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Data;
import org.apache.griffin.api.common.GRPCCode;
import org.apache.griffin.api.proto.protocol.*;

import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.Future;

/**
 * the obj has a socketChannel to worker node
 */
@Data
@Builder
public class DQCConnection {
    // worker hostName
    private String hostName;
    // worker hostPort
    private int hostPort;
    // client
    private ExecuteNodeServiceGrpc.ExecuteNodeServiceBlockingStub client;

    private boolean isAlive;

    public boolean sayHello() throws UnknownHostException {
        SayHelloResponse sayHelloResponse = client.sayHello(SayHelloRequest.newBuilder().build());
        if (sayHelloResponse.getCode() == GRPCCode.SUCCESS.getCode()) {
            return true;
        } else  {
            throw new UnknownHostException(hostName + ":" + hostPort);
        }
    }

    public boolean submitDQTask(Long instanceId) {
        SubmitDQTaskRequest submitDQTaskRequest = SubmitDQTaskRequest.newBuilder()
                .setInstanceId(instanceId)
                .build();
        SubmitDQTaskResponse submitDQTaskResponse = client.submitDQTask(submitDQTaskRequest);
        return submitDQTaskResponse.getCode() == GRPCCode.SUCCESS.getCode();
    }

}
