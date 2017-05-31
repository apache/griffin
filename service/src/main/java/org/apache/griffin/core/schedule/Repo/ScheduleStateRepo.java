package org.apache.griffin.core.schedule.Repo;

import org.apache.griffin.core.schedule.ScheduleState;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by xiangrchen on 5/31/17.
 */
@Repository
public interface ScheduleStateRepo extends CrudRepository<ScheduleState,Long>{
    @Query("select s from ScheduleState s " +
            "where s.groupName= ?1 and s.jobName=?2")
    Iterable<ScheduleState> findAllByGroupNameAndJobName(String group,String name);
}
