package org.apache.griffin.core.measure.repo;


import org.apache.griffin.core.measure.Measure;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface MeasureRepo extends CrudRepository<Measure, Long> {
    Measure findByName(String name);

    @Query("select DISTINCT m.organization from Measure m")
    List<String> findOrganizations();

    @Query("select m.name from Measure m " +
            "where m.organization= ?1")
    List<String> findNameByOrganization(String organization);

    @Query("select m.organization from Measure m "+
            "where m.name= ?1")
    String findOrgByName(String measureName);
}
