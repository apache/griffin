package org.apache.griffin.core.worker.stage;

import org.apache.commons.collections.CollectionUtils;
import org.apache.griffin.core.worker.entity.bo.task.DQBaseTask;
import org.apache.griffin.core.worker.utils.MsgSender;

import java.util.List;
import java.util.stream.Collectors;

public class DQAlertStage extends DQAbstractStage {

    @Override
    public void process() {

        List<String> alertMsgList = subTaskList.stream()
                .filter(DQBaseTask::isNeedAlert)
                .map(DQBaseTask::alert)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(alertMsgList)) {
            return;
        }
        MsgSender.send(packageAlertMsg(alertMsgList), instance.getDqAlertRule().getReceivers(), instance.getDqAlertRule().getSendType());
    }

    private String packageAlertMsg(List<String> alertMsgList) {
        return null;
    }

    @Override
    public boolean hasSuccess() {
        return false;
    }
}
