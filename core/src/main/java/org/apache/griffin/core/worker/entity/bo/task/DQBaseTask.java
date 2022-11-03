package org.apache.griffin.core.worker.entity.bo.task;

import lombok.Data;
import org.apache.griffin.core.worker.context.WorkerContext;
import org.apache.griffin.core.worker.entity.pojo.rule.DQAlertRule;
import org.apache.griffin.core.worker.entity.pojo.rule.DQEvaluateRule;
import org.apache.griffin.core.worker.entity.pojo.rule.DQRecordRule;

/**
 * 任务基础类
 *   一个完整的任务包含
 *      record
 *      evaluate
 *      alert
 */
@Data
public abstract class DQBaseTask {

    private WorkerContext wc;

    private DQRecordRule recordRule;
    private DQEvaluateRule dqEvaluateRule;
    private DQAlertRule dqAlertRule;

    // 生成recordsql和 模板 + 参数 有关系
    // 生成SQL部分希望交给模板来做
    public abstract void doRecord();
    public abstract void doEvaluate();
    public abstract void doAlert();
}
