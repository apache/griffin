/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.measure;


import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.List;

@Service
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

    public List<String> getAllMeasureNameByOwner(String owner){
        List<String> res=new ArrayList<String>();
        for (Measure measure:measureRepo.findAll()){
            if(measure.getOwner().equals(owner)){
                res.add(measure.getName());
            }
        }
        return res;
    }

    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
//        Long measureId=measure.getId();
//        if (measureRepo.findOne(measureId)==null){
//            return GriffinOperationMessage.RESOURCE_NOT_FOUND;
//        }else{
//            measureRepo.updateMeasure(measureId,measure.getDescription(),measure.getOrganization(),measure.getSource(),measure.getTarget(),measure.getEvaluateRule());
////            System.out.print(res);
//            return GriffinOperationMessage.UPDATE_MEASURE_SUCCESS;
//        }
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
