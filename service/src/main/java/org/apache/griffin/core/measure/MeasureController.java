package org.apache.griffin.core.measure;

import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class MeasureController {
    private static final Logger log = LoggerFactory.getLogger(MeasureController.class);
    public enum Message {
        success,
        fail,
        notFound
    }

    @Autowired
    MeasureRepo measureRepo;
    @RequestMapping("/measures")
    public Iterable<Measure> getMeasures() {
        return measureRepo.findAll();
    }

    @RequestMapping("/measures/{id}")
    public Measure getMeasuresById(@PathVariable("id") long id) {
        return measureRepo.findOne(id);
    }

    @RequestMapping("/measures/findByName/{measureName}")
    public Measure getMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureRepo.findByName(measureName);
    }

    @RequestMapping(value = "/measures/deleteById/{MeasureId}",method = RequestMethod.DELETE)
    public void deleteMeasuresById(@PathVariable("MeasureId") Long MeasureId) { measureRepo.delete(MeasureId);}

    @RequestMapping(value = "/measures/deleteByName/{measureName}",method = RequestMethod.DELETE)
    public Message deleteMeasuresByName(@PathVariable("measureName") String measureName) {
        Message msg;
        Measure temp_mesaure=measureRepo.findByName(measureName);
        if(temp_mesaure==null){
            msg=Message.notFound;
        }
        else{
            measureRepo.delete(temp_mesaure.getId());
            msg=Message.success;
        }
        return msg;
    }

    @RequestMapping(value = "/measures/update",method = RequestMethod.POST)
    @ResponseBody
    public MeasureController.Message updateMeasure(@RequestBody Measure measure) {
        MeasureController.Message msg;
        String name=measure.getName();
        Measure temp_mesaure=measureRepo.findByName(name);
        if (temp_mesaure==null){
            msg=Message.notFound;
        }else{
            //in this way, id will changed
            measureRepo.delete(temp_mesaure.getId());
            measureRepo.save(measure);
            msg=Message.success;
        }
        return msg;
    }


    @RequestMapping(value = "/measures/add", method = RequestMethod.POST)
    @ResponseBody
    public Message createNewMeasure(@RequestBody Measure measure) {
        Message msg;
        System.out.println(measure);
        String name=measure.getName();
        Measure temp_mesaure=measureRepo.findByName(name);
        if (temp_mesaure==null){
            if (measureRepo.save(measure)!=null)
                msg=Message.success;
            else{
                msg = Message.fail;
            }
        } else{
            log.info("Failed to create new measure "+name+", it already exists");
            return Message.fail;
        }
        return msg;
    }
}
