package org.apache.griffin.core.metric;

import org.junit.Test;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class MetricStoreImplTest {

    @Test
    public void testBuildBasicAuthString()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = MetricStoreImpl.class.getDeclaredMethod("buildBasicAuthString", String.class, String.class);
        m.setAccessible(true);
        String authStr = (String) m.invoke(null, "user", "password");
        assertTrue(authStr.equals("Basic dXNlcjpwYXNzd29yZA=="));
    }

}