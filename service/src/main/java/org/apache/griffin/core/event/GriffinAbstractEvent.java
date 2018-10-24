package org.apache.griffin.core.event;

public abstract class GriffinAbstractEvent<T> implements GriffinEvent<T> {
    private T source;
    private EventType type;
    private EventSourceType sourceType;
    private EventPointcutType pointcutType;

    public GriffinAbstractEvent(T source,
                                EventType type,
                                EventSourceType sourceType,
                                EventPointcutType pointcutType) {
        this.source = source;
        this.type = type;
        this.sourceType = sourceType;
        this.pointcutType = pointcutType;
    }

    @Override
    public EventType getType() {
        return this.type;
    }

    @Override
    public EventPointcutType getPointcut() {
        return pointcutType;
    }

    @Override
    public EventSourceType getSourceType() {
        return sourceType;
    }

    @Override
    public T getSource() {
        return source;
    }
}
