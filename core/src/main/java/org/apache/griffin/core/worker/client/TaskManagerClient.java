package org.apache.griffin.core.worker.client;


import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.griffin.api.proto.protocol.TaskManagerServiceGrpc;

public class TaskManagerClient {

    // todo connect to master?
    @GrpcClient("taskManagerService")
    private TaskManagerServiceGrpc.TaskManagerServiceBlockingStub client;

    public void test() {

//        TaskManagerServiceGrpc.newBlockingStub()
    }
}
