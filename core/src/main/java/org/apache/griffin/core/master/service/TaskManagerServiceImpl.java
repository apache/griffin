package org.apache.griffin.core.master.service;


import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.griffin.api.common.GRPCCode;
import org.apache.griffin.api.proto.protocol.*;
import org.apache.griffin.core.master.server.TaskManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.UnknownHostException;

@GrpcService
@Slf4j
public class TaskManagerServiceImpl extends TaskManagerServiceGrpc.TaskManagerServiceImplBase {

    @Autowired
    private TaskManager taskManager;

    @Override
    public void registDQWorkerNode(RegistDQWorkerNodeRequest request, StreamObserver<RegistDQWorkerNodeResponse> responseObserver) {
        String hostName = request.getHostName();
        int port = request.getPort();
        GRPCCode code = GRPCCode.SUCCESS;
        try {
            taskManager.registerWorker(hostName, port);
        } catch (UnknownHostException uhe) {
            log.error("TaskManager registerWorker failed. Host: {}, port: {} is unreachable", hostName, port);
            code = GRPCCode.CLIENT_ERROR;
        } catch (Exception e) {
            log.error("TaskManager registerWorker Unknown Error. Host: {}, port: {}", hostName, port, e);
            code = GRPCCode.SERVER_ERROR;
        }
        responseObserver.onNext(RegistDQWorkerNodeResponse.newBuilder()
                .setCode(code.getCode())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void reportDQWorkNodeStatus(ReportDQWorkNodeStatusRequest request, StreamObserver<ReportDQWorkNodeStatusResponse> responseObserver) {
        super.reportDQWorkNodeStatus(request, responseObserver);
    }

    @Override
    public void sayHello(SayHelloRequest request, StreamObserver<SayHelloResponse> responseObserver) {
        responseObserver.onNext(SayHelloResponse.newBuilder()
                .setCode(GRPCCode.SUCCESS.getCode())
                .build());
        responseObserver.onCompleted();
    }
}
