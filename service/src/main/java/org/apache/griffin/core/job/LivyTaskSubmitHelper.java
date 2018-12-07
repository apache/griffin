package org.apache.griffin.core.job;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.griffin.core.job.entity.JobInstanceBean;
import org.apache.griffin.core.job.repo.JobInstanceRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.griffin.core.job.entity.LivySessionStates.State.FOUND;
import static org.apache.griffin.core.util.JsonUtil.toJsonWithFormat;

@Component
public class LivyTaskSubmitHelper {

    private static final Logger logger = LoggerFactory.getLogger(LivyTaskSubmitHelper.class);

    private static final String REQUEST_BY_HEADER = "X-Requested-By";

    private JobInstanceBean jobInstance;

    private SparkSubmitJob sparkSubmitJob;

    private ConcurrentMap<Long, Integer> taskAppIdMap = new ConcurrentHashMap<>();

    // Current number of tasks
    private AtomicInteger curConcurrentTaskNum = new AtomicInteger(0);

    private String workerNamePre;

    private RestTemplate restTemplate = new RestTemplate();

    // queue for pub or sub
    private BlockingQueue<Map<String, Object>> queue;

    public static final int DEFAULT_QUEUE_SIZE = 20000;

    private String uri;


    @Autowired
    private JobInstanceRepo jobInstanceRepo;
    @Autowired
    private Environment env;

    @PostConstruct
    public void init() {
        startWorker();
        uri = env.getProperty("livy.uri");
    }

    public LivyTaskSubmitHelper() {
        this.workerNamePre = "livy-task-submit-worker";
    }

    public void startWorker() {
        queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        BatchTaskWorker worker = new BatchTaskWorker();
        worker.setDaemon(true);
        worker.setName(workerNamePre + "-" + worker.getName());
        worker.start();
    }

    public void addTaskToWaitingQueue(Map<String, Object> t) {
        if (t == null) {
            logger.warn("task is blank, workerNamePre: {}", workerNamePre);
            return;
        }

        if (queue.remainingCapacity() <= 0) {
            logger.warn("task is discard, workerNamePre: {}, task: {}", workerNamePre, JSON.toJSON(t));
            return;
        }

        queue.add(t);

        logger.info("add_task_to_waiting_queue_success, workerNamePre: {}, task: {}", workerNamePre, JSON.toJSON(t));
    }

    /**
     * Consumer thread
     */
    class BatchTaskWorker extends Thread {
        public void run() {
            long insertTime = System.currentTimeMillis();

            // Keep sequential execution within a limited number of tasks
            while (true) {
                try {
                    if (curConcurrentTaskNum.get() < getMaxConcurrentTaskCount()
                            && (System.currentTimeMillis() - insertTime) >= getBatchIntervalSecond() * 1000) {
                        Map<String, Object> task = queue.take();
                        doTask(task);
                        insertTime = System.currentTimeMillis();
                    }
                } catch (Throwable e) {
                    logger.error("Async_worker_doTask_failed, {}", workerNamePre + e.getMessage(), e);
                }
            }
        }
    }

    public void increaseCurTaskNum(Long scheduleId) {
        curConcurrentTaskNum.incrementAndGet();
        if (scheduleId != null) taskAppIdMap.put(scheduleId, 1);
    }

    //Remove tasks after job status updates
    public void decreaseCurTaskNum(Long scheduleId) {
        if (scheduleId != null && taskAppIdMap.containsKey(scheduleId)) {
            curConcurrentTaskNum.decrementAndGet();
            taskAppIdMap.remove(scheduleId);
        }
    }

    /**
     * Submit a task to Livy and concurrent TaskNum++
     */
    protected void doTask(Map<String, Object> livyConfMap) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set(REQUEST_BY_HEADER, "admin");

            HttpEntity<String> springEntity = new HttpEntity<String>(toJsonWithFormat(livyConfMap), headers);

            String result = restTemplate.postForObject(uri, springEntity, String.class);
            logger.info("submit livy result: {}", result);


            Gson gson = new Gson();
            try {
                jobInstance = gson.fromJson(result, JobInstanceBean.class);

                // The first time didn't get appId, try again several times
                if (StringUtils.isBlank(jobInstance.getAppId())) {
                    jobInstance = retryLivyGetAppId(jobInstance);
                }

                logger.info("submit livy scheduleResult: {}", jobInstance);
            } catch (Exception e) {
                logger.error("submit livy scheduleResult covert error!", e);
            }

            if (jobInstance != null) {
                //save result info into DataBase
                sparkSubmitJob.saveJobInstance(result, FOUND);

                // Successful submission of a task
                increaseCurTaskNum(jobInstance.getId());
            }
        } catch (Exception e) {
            logger.error("submit task to livy error.", e);
        }
    }

    private JobInstanceBean retryLivyGetAppId(JobInstanceBean jobInstance) {

        int retryCount = getAppIdRetryCount();

        if (retryCount <= 0) {
            return jobInstance;
        }

        Long livyBatchesId = jobInstance.getId();
        if (livyBatchesId == null) {
            return jobInstance;
        }

        while (retryCount-- > 0) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
            String result = restTemplate.getForObject(uri + "/" + livyBatchesId, String.class);
            logger.info("retry get livy result: {}, batches id : {}", result, livyBatchesId);

            Gson gson = new Gson();
            JobInstanceBean newJobInstance = gson.fromJson(result, JobInstanceBean.class);
            if (StringUtils.isNotBlank(newJobInstance.getAppId())) {
                return newJobInstance;
            }
        }

        return jobInstance;
    }

    // Maximum number of parallel tasks
    protected int getMaxConcurrentTaskCount() {
        return Integer.parseInt(env.getProperty("livy.task.max.concurrent.count"));
    }

    // Submit once every 3 seconds by default
    protected int getBatchIntervalSecond() {
        return Integer.parseInt(env.getProperty("livy.task.submit.interval.second"));
    }

    // Livy can't get the number of retries for appid
    protected int getAppIdRetryCount() {
        return Integer.parseInt(env.getProperty("livy.task.appId.retry.count"));
    }

    // Livy queue select
    protected boolean isNeedLivyQueue() {
        return Boolean.parseBoolean(env.getProperty("livy.need.queue"));
    }

}
