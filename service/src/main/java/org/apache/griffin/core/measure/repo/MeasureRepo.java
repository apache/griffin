/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.measure.repo;


import org.apache.griffin.core.measure.entity.Measure;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MeasureRepo extends CrudRepository<Measure, Long> {
    List<Measure> findByNameAndDeleted(String name, Boolean deleted);

    List<Measure> findByDeleted(Boolean deleted);

    List<Measure> findByOwnerAndDeleted(String owner, Boolean deleted);

    Measure findByIdAndDeleted(Long id, Boolean deleted);

    @Query("select DISTINCT m.organization from Measure m where m.deleted = ?1")
    List<String> findOrganizations(Boolean deleted);

    @Query("select m.name from Measure m " +
            "where m.organization= ?1 and m.deleted= ?2")
    List<String> findNameByOrganization(String organization, Boolean deleted);

    @Query("select m.organization from Measure m " +
            "where m.name= ?1")
    String findOrgByName(String measureName);

//    @Modifying
//    @Transactional
//    @Query("update Measure m "+
//            "set m.description= ?2,m.organization= ?3,m.source= ?4,m.target= ?5,m.evaluateRule= ?6 where m.id= ?1")
//    void update(Long Id, String description, String organization, DataConnector source, DataConnector target, EvaluateRule evaluateRule);
}
