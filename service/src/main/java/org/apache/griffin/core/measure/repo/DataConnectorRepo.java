package org.apache.griffin.core.measure.repo;


import org.apache.griffin.core.measure.DataConnector;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataConnectorRepo extends CrudRepository<DataConnector, Long> {


}
