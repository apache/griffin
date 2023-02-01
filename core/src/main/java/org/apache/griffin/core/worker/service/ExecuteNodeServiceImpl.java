package org.apache.griffin.core.worker.service;


import io.grpc.stub.StreamObserver;
import org.apache.griffin.api.proto.protocol.*;

public class ExecuteNodeServiceImpl extends ExecuteNodeServiceGrpc.ExecuteNodeServiceImplBase {
    @Override
    public void submitDQTask(SubmitDQTaskRequest request, StreamObserver<SubmitDQTaskResponse> responseObserver) {
        super.submitDQTask(request, responseObserver);
    }

    @Override
    public void stopDQTask(StopDQTaskRequest request, StreamObserver<StopDQTaskResponse> responseObserver) {
        super.stopDQTask(request, responseObserver);
    }

    @Override
    public void querySingleDQTask(SQuerySingleDQTaskRequest request, StreamObserver<QuerySingleDQTaskResponse> responseObserver) {
        super.querySingleDQTask(request, responseObserver);
    }
}
