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

package org.apache.griffin.core.measure;

import org.apache.griffin.core.measure.entity.Measure;
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

    @RequestMapping("/measure/{id}")
    public Measure getMeasuresById_(@PathVariable("id") long id) {
        return measureService.getMeasuresById(id);
    }

    @RequestMapping("/measures/findByName/{measureName}")
    public Measure getMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureService.getMeasuresByName(measureName);
    }

    @RequestMapping("/measure")
    public Measure getMeasuresByName_(@RequestParam("measureName") String measureName) {
        return measureService.getMeasuresByName(measureName);
    }

    @RequestMapping(value = "/measures/deleteById/{MeasureId}",method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasuresById(@PathVariable("MeasureId") Long MeasureId) {
        return measureService.deleteMeasuresById(MeasureId);
    }

    @RequestMapping(value = "/measure/{MeasureId}",method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasuresById_(@PathVariable("MeasureId") Long MeasureId) {
        return measureService.deleteMeasuresById(MeasureId);
    }

    @RequestMapping(value = "/measures/deleteByName/{measureName}",method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasuresByName(@PathVariable("measureName") String measureName) {
        return measureService.deleteMeasuresByName(measureName);
    }

    @RequestMapping(value = "/measure",method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasuresByName_(@RequestParam("measureName") String measureName) {
        return measureService.deleteMeasuresByName(measureName);
    }

    @RequestMapping(value = "/measures/update",method = RequestMethod.POST)
    @ResponseBody
    @Transactional
    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
        return measureService.updateMeasure(measure);
    }

    @RequestMapping(value = "/measure",method = RequestMethod.PUT)
    @ResponseBody
    @Transactional
    public GriffinOperationMessage updateMeasure_(@RequestBody Measure measure) {
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

    @RequestMapping(value = "/measure", method = RequestMethod.POST)
    @ResponseBody
    public GriffinOperationMessage createNewMeasure_(@RequestBody Measure measure) {
        return measureService.createNewMeasure(measure);
    }
}
