package org.apache.griffin.core.worker.stage;

import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.entity.enums.DQTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class DQEvaluateStage extends DQAbstractStage {

    private static final Logger log = LoggerFactory.getLogger(DQEvaluateStage.class);

    @Override
    public void process() {
        // failed task will not do evaluate
        List<DQBaseTask> waitToDoEvaluateTaskList = subTaskList.stream()
                .filter(task -> task.getStatus() == DQTaskStatus.RECORDED)
                .collect(Collectors.toList());
        dqTaskService.updateTaskStatus(waitToDoEvaluateTaskList, DQTaskStatus.EVALUATING);
        waitToDoEvaluateTaskList.forEach(task -> {
            // retry 3 times
            for (int i = 0; i < 3; i++) {
                try {
                    task.evaluate();
                    dqTaskService.updateTaskStatus(task, DQTaskStatus.EVALUATED);
                    break;
                } catch (Exception e) {
                    log.error("Evaluate failed {} times", i);
                }
            }
            if (task.getStatus() != DQTaskStatus.EVALUATED) {
                dqTaskService.updateTaskStatus(task, DQTaskStatus.FAILED);
            }
        });
    }

    @Override
    public boolean hasSuccess() {
        for (DQBaseTask task : subTaskList) {
            if (task.getStatus() == DQTaskStatus.EVALUATED) {
                return true;
            }
        }
        return false;
    }
}
