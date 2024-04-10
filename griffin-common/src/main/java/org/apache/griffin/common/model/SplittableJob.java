package org.apache.griffin.common.model;

import org.apache.griffin.common.exception.JobException;

import java.util.List;

public interface SplittableJob {
    List<IJob> split(String jobParams) throws JobException;
}
