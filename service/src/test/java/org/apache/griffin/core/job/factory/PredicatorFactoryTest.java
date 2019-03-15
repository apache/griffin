package org.apache.griffin.core.job.factory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.griffin.core.exception.GriffinException;
import org.apache.griffin.core.job.FileExistPredicator;
import org.apache.griffin.core.job.Predicator;
import org.apache.griffin.core.job.entity.SegmentPredicate;
import org.apache.griffin.core.util.PredicatorMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;

import static org.apache.griffin.core.util.EntityMocksHelper.createFileExistPredicate;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
public class PredicatorFactoryTest {

    @Test
    public void testFileExistPredicatorCreation() throws IOException {
        Predicator predicator = PredicatorFactory.newPredicateInstance(createFileExistPredicate());
        assertNotNull(predicator);
        assertTrue(predicator instanceof FileExistPredicator);
    }

    @Test(expected = GriffinException.NotFoundException.class)
    public void testUnknownPredicator() throws JsonProcessingException {
        PredicatorFactory.newPredicateInstance(
                new SegmentPredicate("unknown", null));
    }

    @Test
    public void testPluggablePredicator() throws JsonProcessingException {
        String predicatorClass = "org.apache.griffin.core.util.PredicatorMock";
        HashMap<String, Object> map = new HashMap<>();
        map.put("class", predicatorClass);
        SegmentPredicate segmentPredicate = new SegmentPredicate("custom", null);
        segmentPredicate.setConfigMap(map);
        Predicator predicator = PredicatorFactory.newPredicateInstance(segmentPredicate);
        assertNotNull(predicator);
        assertTrue(predicator instanceof PredicatorMock);
    }
}
