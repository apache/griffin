package org.apache.griffin.core.event;

import org.apache.griffin.core.job.entity.AbstractJob;

public class JobEvent extends GriffinAbstractEvent<AbstractJob> {

    private JobEvent(AbstractJob source,
                     EventType type,
                     EventSourceType sourceType,
                     EventPointcutType pointcutType) {
        super(source, type, sourceType, pointcutType);
    }

    public static JobEvent yieldJobEventBeforeCreation(AbstractJob source) {
        return new JobEvent(source,
                EventType.CREATION_EVENT,
                EventSourceType.JOB,
                EventPointcutType.BEFORE);
    }

    public static JobEvent yieldJobEventAfterCreation(AbstractJob source) {
        return new JobEvent(source,
                EventType.CREATION_EVENT,
                EventSourceType.JOB,
                EventPointcutType.AFTER);
    }

    public static JobEvent yieldJobEventBeforeRemoval(AbstractJob source) {
        return new JobEvent(source,
                EventType.REMOVAL_EVENT,
                EventSourceType.JOB,
                EventPointcutType.BEFORE);
    }

    public static JobEvent yieldJobEventAfterRemoval(AbstractJob source) {
        return new JobEvent(source,
                EventType.REMOVAL_EVENT,
                EventSourceType.JOB,
                EventPointcutType.AFTER);
    }
}
