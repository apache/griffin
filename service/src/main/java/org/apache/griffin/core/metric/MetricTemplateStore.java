package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.Measure;

/**
 * Proxy class to manage metric templates, return true/false if process succeed/failed.
 */
public interface MetricTemplateStore {

    Boolean createFromMeasure(ExternalMeasure measure);

    Boolean updateFromMeasure(ExternalMeasure measure);

    Boolean deleteFromMeasure(ExternalMeasure measure);

    Boolean createFromJob(Measure measure, String jobId, String jobName);

    Boolean deleteFromJob(String jobId, String jobName);
}
