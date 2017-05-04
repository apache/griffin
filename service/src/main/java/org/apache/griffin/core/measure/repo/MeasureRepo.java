package org.apache.griffin.core.measure.repo;


import org.apache.griffin.core.measure.Measure;
import org.springframework.data.repository.CrudRepository;

public interface MeasureRepo extends CrudRepository<Measure, Long> {
    Measure findByName(String name);
}
