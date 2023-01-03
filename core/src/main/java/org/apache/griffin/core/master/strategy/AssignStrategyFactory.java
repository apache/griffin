package org.apache.griffin.core.master.strategy;

public class AssignStrategyFactory {

    public static AbstractAssignStrategy getStrategy(String className) {
        try {
            Class clazz = Class.forName(className);
            return (AbstractAssignStrategy) clazz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
