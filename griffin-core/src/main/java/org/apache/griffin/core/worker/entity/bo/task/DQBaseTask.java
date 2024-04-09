package org.apache.griffin.core.worker.entity.bo.task;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.worker.context.WorkerContext;
import org.apache.griffin.core.worker.entity.dispatcher.JobStatus;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;

import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.entity.pojo.Metric;
import org.apache.griffin.core.worker.entity.pojo.rule.DQAlertRule;
import org.apache.griffin.core.worker.entity.pojo.rule.DQEvaluateRule;
import org.apache.griffin.core.worker.entity.pojo.rule.DQRecordRule;

import java.util.ArrayList;
import java.util.List;

/**
 *      record
 *      evaluate
 *      alert
 */
@Data
public abstract class DQBaseTask {

    private long id;
    private String owner;
    private WorkerContext wc;
    private DQEngineEnum engine;
    private DQRecordRule recordRule;
    private DQEvaluateRule dqEvaluateRule;
    private DQAlertRule dqAlertRule;
    private DQTaskStatus status;
    private List<JobStatus> jobStatusList;
    private List<Metric> metricList = new ArrayList<>();
    // 记录状态年龄  状态更新是重置
    private int statusAge;
    // 生成recordsql和 模板 + 参数 有关系
    // 生成SQL部分希望交给模板来做
    public List<Pair<Long, String>> record() {
        // before
        List<Pair<Long, String>> partitionTimeAndSqlList = doRecord();
        // after
        return partitionTimeAndSqlList;
    }
    public void evaluate() {
        doEvaluate();
    }
    public void alert() {
        doAlert();
    }

    // partitionTime And sql
    public abstract List<Pair<Long, String>> doRecord();
    public abstract boolean doEvaluate();
    public abstract boolean doAlert();

    public void setStatus(DQTaskStatus status) {
        if (this.status != status) resetStatusAge();
        this.status = status;
    }

    public void resetStatusAge() {
        statusAge = 0;
    }

    public void incrStatusAge() {
        statusAge++;
    }

    public boolean isFailed() {
        // 一个状态年龄处理了5次 无法变更 说明处理该任务一直失败
        return statusAge > 5;
    }

    public void addMetric(long partitionTime, Double metric) {
        metricList.add(new Metric(partitionTime, metric));
    }
}
