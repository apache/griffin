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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/api/v1")
public class MeasureController {
    @Autowired
    private MeasureService measureService;

    @RequestMapping(value = "/measures", method = RequestMethod.GET)
    public Iterable<Measure> getAllAliveMeasures() {
        return measureService.getAllAliveMeasures();
    }

    @RequestMapping(value = "/measure/{id}", method = RequestMethod.GET)
    public Measure getMeasureById(@PathVariable("id") long id) {
        return measureService.getMeasureById(id);
    }

    @RequestMapping(value = "/measure/{id}", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasureById(@PathVariable("id") Long id) {
        return measureService.deleteMeasureById(id);
    }

    @RequestMapping(value = "/measure", method = RequestMethod.PUT)
    public GriffinOperationMessage updateMeasure(@RequestBody Measure measure) {
        return measureService.updateMeasure(measure);
    }

    @RequestMapping(value = "/measures/owner/{owner}", method = RequestMethod.GET)
    public List<Measure> getAliveMeasuresByOwner(@PathVariable("owner") String owner) {
        return measureService.getAliveMeasuresByOwner(owner);
    }

    @RequestMapping(value = "/measure", method = RequestMethod.POST)
    public GriffinOperationMessage createMeasure(@RequestBody Measure measure) {
        return measureService.createMeasure(measure);
    }
}
