package org.apache.griffin.core.api.utils;


import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringUtils implements ApplicationContextAware {
    // Spring context
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringUtils.applicationContext = applicationContext;
    }

    public static <T> T getObject(String name, Class<T> clazz) {
        Object bean = applicationContext.getBean(name);
        if (clazz.isInstance(bean)) return clazz.cast(bean);
        throw new BeanNotOfRequiredTypeException(name, clazz, bean.getClass());
    }
}
