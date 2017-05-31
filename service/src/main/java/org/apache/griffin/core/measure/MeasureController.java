package org.apache.griffin.core.measure;

import org.apache.griffin.core.util.GriffinOperationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class MeasureController {
    private static final Logger log = LoggerFactory.getLogger(MeasureController.class);

    @Autowired
    MeasureService measureService;
    @RequestMapping("/measures")
    public Iterable<Measure> getAllMeasures() {
        return measureService.getAllMeasures();
    }

    @RequestMapping("/measures/{id}")
    public Measure getMeasuresById(@PathVariable("id") long id) {
        return measureService.getMeasuresById(id);
    }

    @RequestMapping("/measures/findByName/{measureName}")
    public Measure getMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureService.getMeasuresByName(measureName);
    }

    @RequestMapping(value = "/measures/deleteById/{MeasureId}",method = RequestMethod.DELETE)
    public void deleteMeasuresById(@PathVariable("MeasureId") Long MeasureId) { measureService.deleteMeasuresById(MeasureId);}

    @RequestMapping(value = "/measures/deleteByName/{measureName}",method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureService.deleteMeasuresByName(measureName);
    }

    @RequestMapping(value = "/measures/update",method = RequestMethod.POST)
    @ResponseBody
    @Transactional
    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
        return measureService.updateMeasure(measure);
    }

    @RequestMapping("/measures/owner/{owner}")
    public List<String> getAllMeasureNameOfOwner(@PathVariable("owner") String owner){
        return measureService.getAllMeasureNameByOwner(owner);
    }

    @RequestMapping(value = "/measures/add", method = RequestMethod.POST)
    @ResponseBody
    public GriffinOperationMessage createNewMeasure(@RequestBody Measure measure) {
        return measureService.createNewMeasure(measure);
    }
}
