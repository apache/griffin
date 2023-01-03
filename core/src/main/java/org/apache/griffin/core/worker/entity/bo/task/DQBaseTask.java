package org.apache.griffin.core.worker.entity.bo.task;

import com.beust.jcommander.internal.Lists;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.griffin.core.api.context.WorkerContext;
import org.apache.griffin.core.worker.entity.dispatcher.JobStatus;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.apache.griffin.core.worker.entity.pojo.Metric;
import org.apache.griffin.core.worker.entity.pojo.rule.DQEvaluateRule;
import org.apache.griffin.core.worker.entity.pojo.rule.DQRecordRule;

import java.util.List;

/**
 * 任务基础类
 *   一个完整的任务包含
 *      record
 *      evaluate
 */
@Data
public abstract class DQBaseTask {

    protected long id;
    protected String owner;
    protected WorkerContext wc;
    protected DQEngineEnum engine;
    protected DQRecordRule recordRule;
    protected DQEvaluateRule dqEvaluateRule;
    protected DQTaskStatus status;
    protected List<JobStatus> jobStatusList;
    protected List<Metric> metricList = Lists.newArrayList();
    protected boolean needAlert = false;
    protected Long businessTime;
    // 记录状态年龄  状态更新是重置
    private int statusAge;
    // 生成recordsql和 模板 + 参数 有关系
    // 生成SQL部分希望交给模板来做
    public List<Pair<Long, String>> record() {
        // before
        List<Pair<Long, String>> partitionTimeAndSqlList = getRecordInfo();
        // after
        return partitionTimeAndSqlList;
    }

    public void evaluate() {
        needAlert = doEvaluate();
    }

    public String alert() {
        return getAlertMsg();
    }

    // partitionTime And sql
    public abstract List<Pair<Long, String>> getRecordInfo();

    public boolean doEvaluate() {
        return dqEvaluateRule.execute(metricList);
    }

    public String getAlertMsg() {
        return "xxx is error";
    }

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
