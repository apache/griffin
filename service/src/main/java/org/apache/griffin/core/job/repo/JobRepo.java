package org.apache.griffin.core.job.repo;

import org.apache.griffin.core.job.entity.Job;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepo<T extends Job> extends CrudRepository<T, Long> {

    List<T> findByDeleted(Boolean deleted);

    List<T> findByNameAndDeleted(String name, Boolean deleted);

    List<T> findByMeasureIdAndDeleted(Long measureId, Boolean deleted);

    List<T> findByMetricNameAndDeleted(String metricName, Boolean deleted);

    T findByIdAndDeleted(Long id, Boolean deleted);
}
