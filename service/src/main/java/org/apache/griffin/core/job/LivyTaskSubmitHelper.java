/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import static org.apache.griffin.core.config.PropertiesConfig.livyConfMap;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.NOT_FOUND;
import static org.apache.griffin.core.util.JsonUtil.toEntity;
import static org.apache.griffin.core.util.JsonUtil.toJsonWithFormat;

import org.apache.commons.collections.map.HashedMap;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.kerberos.client.KerberosRestTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class LivyTaskSubmitHelper {

    private static final Logger logger = LoggerFactory.getLogger(LivyTaskSubmitHelper.class);

    private static final String REQUEST_BY_HEADER = "X-Requested-By";
    private SparkSubmitJob sparkSubmitJob;
    private ConcurrentMap<Long, Integer> taskAppIdMap = new ConcurrentHashMap<>();
    // Current number of tasks
    private AtomicInteger curConcurrentTaskNum = new AtomicInteger(0);
    private String workerNamePre;
    private RestTemplate restTemplate = new RestTemplate();
    // queue for pub or sub
    private BlockingQueue<JobDetail> queue;
    public static final int DEFAULT_QUEUE_SIZE = 20000;
    private static final int SLEEP_TIME = 300;
    private String uri;

    @Value("${livy.task.max.concurrent.count:20}")
    private int maxConcurrentTaskCount;
    @Value("${livy.task.submit.interval.second:3}")
    private int batchIntervalSecond;


    @Autowired
    private Environment env;

    /**
     * Initialize related parameters and open consumer threads.
     */
    @PostConstruct
    public void init() {
        startWorker();
        uri = env.getProperty("livy.uri");
        logger.info("Livy uri : {}", uri);
    }

    public LivyTaskSubmitHelper() {
        this.workerNamePre = "livy-task-submit-worker";
    }

    /**
     * Initialize blocking queues and start consumer threads.
     */
    public void startWorker() {
        queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        TaskInner taskInner = new TaskInner(executorService);
        executorService.execute(taskInner);
    }

    /**
     * Put job detail into the queue.
     *
     * @param jd job detail.
     */
    public void addTaskToWaitingQueue(JobDetail jd) throws IOException {
        if (jd == null) {
            logger.warn("task is blank, workerNamePre: {}", workerNamePre);
            return;
        }

        if (queue.remainingCapacity() <= 0) {
            logger.warn("task is discard, workerNamePre: {}, task: {}", workerNamePre, jd);
            sparkSubmitJob.saveJobInstance(null, NOT_FOUND);
            return;
        }

        queue.add(jd);

        logger.info("add_task_to_waiting_queue_success, workerNamePre: {}, task: {}",
                workerNamePre, jd);
    }

    /**
     * Consumer thread.
     */
    class TaskInner implements Runnable {
        private ExecutorService es;

        public TaskInner(ExecutorService es) {
            this.es = es;
        }

        public void run() {
            long insertTime = System.currentTimeMillis();
            while (true) {
                try {
                    if (curConcurrentTaskNum.get() < maxConcurrentTaskCount
                            && (System.currentTimeMillis() - insertTime) >= batchIntervalSecond * 1000) {
                        JobDetail jd = queue.take();
                        sparkSubmitJob.saveJobInstance(jd);
                        insertTime = System.currentTimeMillis();
                    } else {
                        Thread.sleep(SLEEP_TIME);
                    }
                } catch (Exception e) {
                    logger.error("Async_worker_doTask_failed, {}", e.getMessage(), e);
                    es.execute(this);
                }
            }
        }
    }

    /**
     * Add the batch id returned by Livy.
     *
     * @param scheduleId livy batch id.
     */
    public void increaseCurTaskNum(Long scheduleId) {
        curConcurrentTaskNum.incrementAndGet();
        if (scheduleId != null) {
            taskAppIdMap.put(scheduleId, 1);
        }
    }

    /**
     * Remove tasks after job status updates.
     *
     * @param scheduleId livy batch id.
     */
    public void decreaseCurTaskNum(Long scheduleId) {
        if (scheduleId != null && taskAppIdMap.containsKey(scheduleId)) {
            curConcurrentTaskNum.decrementAndGet();
            taskAppIdMap.remove(scheduleId);
        }
    }

    protected Map<String, Object> retryLivyGetAppId(String result, int appIdRetryCount)
            throws IOException {

        int retryCount = appIdRetryCount;
        TypeReference<HashMap<String, Object>> type =
                new TypeReference<HashMap<String, Object>>() {
                };
        Map<String, Object> resultMap = toEntity(result, type);

        if (retryCount <= 0) {
            return null;
        }

        if (resultMap.get("appId") != null) {
            return resultMap;
        }

        Object livyBatchesId = resultMap.get("id");
        if (livyBatchesId == null) {
            return resultMap;
        }

        while (retryCount-- > 0) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            resultMap = getResultByLivyId(livyBatchesId, type);
            logger.info("retry get livy resultMap: {}, batches id : {}", resultMap, livyBatchesId);

            if (resultMap.get("appId") != null) {
                break;
            }
        }

        return resultMap;
    }

    private Map<String, Object> getResultByLivyId(Object livyBatchesId, TypeReference<HashMap<String, Object>> type)
            throws IOException {
        Map<String, Object> resultMap = new HashedMap();
        String livyUri = uri + "/" + livyBatchesId;
        String result = getFromLivy(livyUri);
        logger.info(result);
        return result == null ? resultMap : toEntity(result, type);
    }

    public String postToLivy(String uri) {
        String needKerberos = env.getProperty("livy.need.kerberos");
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(REQUEST_BY_HEADER,"admin");

        if (needKerberos == null) {
            logger.error("The property \"livy.need.kerberos\" is empty");
            return null;
        }

        if (needKerberos.equalsIgnoreCase("false")) {
            logger.info("The livy server doesn't need Kerberos Authentication");
            String result = null;
            try {

                HttpEntity<String> springEntity = new HttpEntity<>(toJsonWithFormat(livyConfMap),headers);
                result = restTemplate.postForObject(uri,springEntity,String.class);

                logger.info(result);
            } catch (JsonProcessingException e) {
                logger.error("Post to livy ERROR. \n {}", e.getMessage());
            }
            return result;
        } else {
            logger.info("The livy server needs Kerberos Authentication");
            String userPrincipal = env.getProperty("livy.server.auth.kerberos.principal");
            String keyTabLocation = env.getProperty("livy.server.auth.kerberos.keytab");
            logger.info("principal:{}, lcoation:{}", userPrincipal, keyTabLocation);

            KerberosRestTemplate restTemplate = new KerberosRestTemplate(keyTabLocation, userPrincipal);
            HttpEntity<String> springEntity = null;
            try {
                springEntity = new HttpEntity<>(toJsonWithFormat(livyConfMap), headers);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            String result = restTemplate.postForObject(uri, springEntity, String.class);
            logger.info(result);
            return result;
        }
    }

    public String getFromLivy(String uri) {
        String needKerberos = env.getProperty("livy.need.kerberos");

        if (needKerberos == null) {
            logger.error("The property \"livy.need.kerberos\" is empty");
            return null;
        }

        if (needKerberos.equalsIgnoreCase("false")) {
            logger.info("The livy server doesn't need Kerberos Authentication");
            return restTemplate.getForObject(uri, String.class);
        } else {
            logger.info("The livy server needs Kerberos Authentication");
            String userPrincipal = env.getProperty("livy.server.auth.kerberos.principal");
            String keyTabLocation = env.getProperty("livy.server.auth.kerberos.keytab");
            logger.info("principal:{}, lcoation:{}", userPrincipal, keyTabLocation);

            KerberosRestTemplate restTemplate = new KerberosRestTemplate(keyTabLocation, userPrincipal);
            String result = restTemplate.getForObject(uri, String.class);
            logger.info(result);
            return result;
        }
    }

    public void deleteByLivy(String uri) {
        String needKerberos = env.getProperty("livy.need.kerberos");

        if (needKerberos == null) {
            logger.error("The property \"livy.need.kerberos\" is empty");
            return;
        }

        if (needKerberos.equalsIgnoreCase("false")) {
            logger.info("The livy server doesn't need Kerberos Authentication");
            new RestTemplate().delete(uri);
        } else {
            logger.info("The livy server needs Kerberos Authentication");
            String userPrincipal = env.getProperty("livy.server.auth.kerberos.principal");
            String keyTabLocation = env.getProperty("livy.server.auth.kerberos.keytab");
            logger.info("principal:{}, lcoation:{}", userPrincipal, keyTabLocation);

            KerberosRestTemplate restTemplate = new KerberosRestTemplate(keyTabLocation, userPrincipal);
            restTemplate.delete(uri);
        }
    }
}
