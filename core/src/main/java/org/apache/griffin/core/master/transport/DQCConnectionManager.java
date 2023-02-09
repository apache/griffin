package org.apache.griffin.core.master.transport;

import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.griffin.api.proto.protocol.ExecuteNodeServiceGrpc;
import org.apache.griffin.core.master.strategy.AbstractAssignStrategy;
import org.apache.griffin.core.master.strategy.AssignStrategyFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DQCConnectionManager {

    private List<DQCConnection> clientList = Lists.newCopyOnWriteArrayList();
    private String assignStrategyClassName = "org.apache.griffin.core.master.strategy.LooperAssignStrategy";
    private AbstractAssignStrategy strategy;
    @PostConstruct
    public void init() {
        strategy = AssignStrategyFactory.getStrategy(assignStrategyClassName);
        if (strategy == null) {
            log.error("DQCConnectionManager init error.");
            System.exit(-1);
        }
    }

    public void registerWorker(String hostName, int port) throws UnknownHostException {
        try {
            // the client connect to en
            ManagedChannel channel = ManagedChannelBuilder.forAddress(hostName, port)
                    .usePlaintext()
                    .build();
            ExecuteNodeServiceGrpc.ExecuteNodeServiceBlockingStub clientStub = ExecuteNodeServiceGrpc.newBlockingStub(channel);

            DQCConnection dqcConnection = DQCConnection.builder()
                    .hostName(hostName)
                    .hostPort(port)
                    .client(clientStub)
                    .build();
            // check Connection is alive
            if (dqcConnection.sayHello()) {
                clientList.add(dqcConnection);
            }
        } catch (UnknownHostException uhe) {
            throw uhe;
        } catch (Exception e) {
            log.error("Connect to ExecuteNode Failed. Host: {}, Port: {}", hostName, port);
        }
    }

    public DQCConnection getAliveClient() {
        return strategy.assignTask(clientList);
    }

    // check client health
    @Scheduled(cron = "0 * * * * ?")
    public void chekClientHealth() {
        if (CollectionUtils.isEmpty(clientList)) {
            log.warn("There is no dqcConnection, please start up ExecuteNode.");
            return;
        }
        clientList.forEach(client -> {
            String hostName = client.getHostName();
            int hostPort = client.getHostPort();
            try {
                if (!client.isAlive()) {
                    ManagedChannel channel = ManagedChannelBuilder.forAddress(hostName, hostPort)
                            .usePlaintext()
                            .build();
                    ExecuteNodeServiceGrpc.ExecuteNodeServiceBlockingStub clientStub = ExecuteNodeServiceGrpc.newBlockingStub(channel);
                    client.setClient(clientStub);
                }
                if (client.sayHello()) {
                    client.setAlive(true);
                }
            } catch (UnknownHostException e) {
                log.error("Connect to ExecuteNode Failed. Host: {}, Port: {}",
                        hostName, hostPort);
                client.setAlive(false);
            }
        });
    }
}
