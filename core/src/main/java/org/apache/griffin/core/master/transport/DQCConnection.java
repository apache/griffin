package org.apache.griffin.core.master.transport;

import lombok.Builder;
import lombok.Data;
import org.apache.griffin.api.common.GRPCCode;
import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.api.proto.protocol.*;

import java.net.UnknownHostException;

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

    public DQInstanceStatus querySingleDQTask(Long instanceId) throws Exception {
        QuerySingleDQTaskRequest querySingleDQTaskRequest = QuerySingleDQTaskRequest.newBuilder()
                .setInstanceId(instanceId)
                .build();
        QuerySingleDQTaskResponse querySingleDQTaskResponse = client.querySingleDQTask(querySingleDQTaskRequest);
        if (querySingleDQTaskResponse.getCode() == GRPCCode.SUCCESS.getCode()) {
            int status = querySingleDQTaskResponse.getStatus();
            DQInstanceStatus dqInstanceStatus = DQInstanceStatus.findByCode(status);
            return dqInstanceStatus;
        }
        return null;
    }

    public boolean stopDQTask(Long instanceId) {
        StopDQTaskRequest stopDQTaskRequest = StopDQTaskRequest.newBuilder()
                .setInstanceId(instanceId)
                .build();
        StopDQTaskResponse stopDQTaskResponse = client.stopDQTask(stopDQTaskRequest);
        return stopDQTaskResponse.getCode() == GRPCCode.SUCCESS.getCode();
    }
}
