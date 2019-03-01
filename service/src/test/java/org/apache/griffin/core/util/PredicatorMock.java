package org.apache.griffin.core.util;

import org.apache.griffin.core.job.Predicator;
import org.apache.griffin.core.job.entity.SegmentPredicate;

import java.io.IOException;

public class PredicatorMock implements Predicator {
    public PredicatorMock(SegmentPredicate segmentPredicate) {
    }

    @Override
    public boolean predicate() throws IOException {
        return false;
    }
}
