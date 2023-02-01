package org.apache.griffin.core.worker.client;


import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.griffin.api.proto.protocol.TaskManagerServiceGrpc;

public class TaskManagerClient {

    @GrpcClient("taskManagerService")
    private TaskManagerServiceGrpc.TaskManagerServiceBlockingStub client;

    public void test() {
        client.registDQWorkerNode(null);
    }
}
