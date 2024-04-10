package org.apache.griffin.common.model;

public interface IJob {
    /**
     * TODO EXECUTE RESULT
     * @param params
     */
    void execute(IJobParams params, Checkpoint checkpoint);
}
