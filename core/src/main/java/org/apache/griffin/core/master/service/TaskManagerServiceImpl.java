package org.apache.griffin.core.master.service;


import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.griffin.api.proto.protocol.*;

@GrpcService
public class TaskManagerServiceImpl extends TaskManagerServiceGrpc.TaskManagerServiceImplBase {

    @Override
    public void registDQWorkerNode(RegistDQWorkerNodeRequest request, StreamObserver<RegistDQWorkerNodeResponse> responseObserver) {
        String hostName = request.getHostName();
        int port = request.getPort();
        System.out.println(hostName);
        System.out.println(port);
        responseObserver.onNext(RegistDQWorkerNodeResponse.newBuilder()
                .setCode(404)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void reportDQWorkNodeStatus(ReportDQWorkNodeStatusRequest request, StreamObserver<ReportDQWorkNodeStatusResponse> responseObserver) {
        super.reportDQWorkNodeStatus(request, responseObserver);
    }
}
