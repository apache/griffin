package org.apache.griffin.core.worker.entity.task;

import org.apache.griffin.core.worker.context.WorkerContext;

/**
 * 任务基础类
 */
public abstract class DQBaseTask {

    private WorkerContext wc;

    // 生成recordsql和 模板 + 参数 有关系
    // 生成SQL部分希望交给模板来做

//    public void record() {
//        // before
//        doRecord();
//        // after
//    }
//
//    public void evaluate() {
//        // before
//        doEvaluate();
//        // after
//    }
//
//    public void alert() {
//        // before
//        doAlert();
//        // after
//    }

    public abstract void doRecord();
    public abstract void doEvaluate();
    public abstract void doAlert();
}
