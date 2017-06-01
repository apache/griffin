package org.apache.griffin.core.schedule.Repo;

import org.apache.griffin.core.schedule.ScheduleState;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by xiangrchen on 5/31/17.
 */
@Repository
public interface ScheduleStateRepo extends CrudRepository<ScheduleState,Long>{
    @Query("select s from ScheduleState s " +
            "where s.groupName= ?1 and s.jobName=?2 "/*+
            "order by s.timestamp desc"*/)
    List<ScheduleState> findByGroupNameAndJobName(String group, String name, Pageable pageable);
}
