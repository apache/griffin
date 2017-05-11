package org.apache.griffin.core.measure;

import org.junit.Assert;
import org.junit.Test;

import javax.persistence.Entity;


/**
 * Created by xiangrchen on 5/10/17.
 */
public class DataConnectorTest {
    @Test
    public void typeAnnotations() {
        // assert
        AssertAnnotations.assertType(
                DataConnector.class, Entity.class);
    }

    @Test
    public void fieldAnnotations() {
        // assert
        AssertAnnotations.assertField(DataConnector.class, "version");
        AssertAnnotations.assertField(DataConnector.class, "config");
    }

    @Test
    public void entity() {
        // setup
        Entity a = ReflectTool.getClassAnnotation(DataConnector.class, Entity.class);
        // assert
        Assert.assertEquals("", a.name());
    }

//    @Test
//    public void config() {
//        // setup
//        Column c = ReflectTool.getMethodAnnotation(
//                DataConnector.class, "getConfig", Column.class);
//        // assert
//        Assert.assertEquals("config", c.name());
//    }
}
