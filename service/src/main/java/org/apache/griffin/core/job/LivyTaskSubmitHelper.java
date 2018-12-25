package org.apache.griffin.core.job;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import static org.apache.griffin.core.job.entity.LivySessionStates.State.NOT_FOUND;
import static org.apache.griffin.core.util.JsonUtil.toEntity;
import org.apache.commons.collections.map.HashedMap;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class LivyTaskSubmitHelper {

    private static final Logger logger = LoggerFactory
            .getLogger(LivyTaskSubmitHelper.class);

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
        BatchTaskWorker worker = new BatchTaskWorker();
        worker.setDaemon(true);
        worker.setName(workerNamePre + "-" + worker.getName());
        worker.start();
    }

    /**
     * Put job detail into the queue.
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
    class BatchTaskWorker extends Thread {
        public void run() {
            long insertTime = System.currentTimeMillis();

            // Keep sequential execution within a limited number of tasks
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
                } catch (Throwable e) {
                    logger.error("Async_worker_doTask_failed, {}",
                            workerNamePre + e.getMessage(), e);
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
                new TypeReference<HashMap<String, Object>>() {};
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
                Thread.sleep(300);
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
        String newResult = restTemplate.getForObject(uri + "/" + livyBatchesId, String.class);
        return newResult == null ? resultMap : toEntity(newResult, type);
    }

}
