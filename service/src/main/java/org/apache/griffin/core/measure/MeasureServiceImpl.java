package org.apache.griffin.core.measure;


import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

public class MeasureServiceImpl implements MeasureService{
    private static final Logger log = LoggerFactory.getLogger(MeasureServiceImpl.class);

    @Autowired
    MeasureRepo measureRepo;

    public Iterable<Measure> getAllMeasures() {
        return measureRepo.findAll();
    }

    public Measure getMeasuresById(@PathVariable("id") long id) {
        return measureRepo.findOne(id);
    }

    public Measure getMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureRepo.findByName(measureName);
    }

    public void deleteMeasuresById(@PathVariable("MeasureId") Long MeasureId) { measureRepo.delete(MeasureId);}


    public GriffinOperationMessage deleteMeasuresByName(@PathVariable("measureName") String measureName) {
        Measure temp_mesaure=measureRepo.findByName(measureName);
        if(temp_mesaure==null){
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }
        else{
            measureRepo.delete(temp_mesaure.getId());
            return GriffinOperationMessage.DELETE_MEASURE_BY_NAME_SUCCESS;
        }
    }

    public GriffinOperationMessage createNewMeasure(@RequestBody Measure measure) {
        String name=measure.getName();
        Measure temp_mesaure=measureRepo.findByName(name);
        if (temp_mesaure==null){
            if (measureRepo.save(measure)!=null)
                return GriffinOperationMessage.CREATE_MEASURE_SUCCESS;
            else{
                return GriffinOperationMessage.CREATE_MEASURE_FAIL;
            }
        } else{
            log.info("Failed to create new measure "+name+", it already exists");
            return GriffinOperationMessage.CREATE_MEASURE_FAIL_DUPLICATE;
        }
    }

    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {

        String name=measure.getName();
        Measure temp_mesaure=measureRepo.findByName(name);
        if (temp_mesaure==null){
            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
        }else{
            //in this way, id will changed
            //TODO, FRONTEND ID?
            measureRepo.delete(temp_mesaure.getId());
            measureRepo.save(measure);
            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
        }
    }
}
