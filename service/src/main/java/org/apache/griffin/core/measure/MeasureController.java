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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.util.GriffinOperationMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Api(tags = "Measures",description = "measure data quality between source and target dataset")
@RestController
@RequestMapping(value = "/api/v1")
public class MeasureController {
    @Autowired
    private MeasureService measureService;

    @ApiOperation(value ="Get measures",response = Iterable.class)
    @RequestMapping(value = "/measures", method = RequestMethod.GET)
    public Iterable<Measure> getAllAliveMeasures() {
        return measureService.getAllAliveMeasures();
    }

    @ApiOperation(value ="Get measure by id",response = Measure.class)
    @RequestMapping(value = "/measure/{id}", method = RequestMethod.GET)
    public Measure getMeasureById(@ApiParam(value = "measure id", required = true)  @PathVariable("id") long id) {
        return measureService.getMeasureById(id);
    }

    @ApiOperation(value ="Delete measure",response = GriffinOperationMessage.class)
    @RequestMapping(value = "/measure/{id}", method = RequestMethod.DELETE)
    public GriffinOperationMessage deleteMeasureById(@ApiParam(value = "measure id", required = true) @PathVariable("id") Long id) {
        return measureService.deleteMeasureById(id);
    }
    @ApiOperation(value ="Update measure",response = GriffinOperationMessage.class)
    @RequestMapping(value = "/measure", method = RequestMethod.PUT)
    public GriffinOperationMessage updateMeasure(@ApiParam(value = "measure entity", required = true) @RequestBody Measure measure) {
        return measureService.updateMeasure(measure);
    }

    @ApiOperation(value ="Get measures by owner",response = List.class)
    @RequestMapping(value = "/measures/owner/{owner}", method = RequestMethod.GET)
    public List<Measure> getAliveMeasuresByOwner(@ApiParam(value = "owner name", required = true) @PathVariable("owner") String owner) {
        return measureService.getAliveMeasuresByOwner(owner);
    }

    @ApiOperation(value ="Add measure",response = GriffinOperationMessage.class)
    @RequestMapping(value = "/measure", method = RequestMethod.POST)
    public GriffinOperationMessage createMeasure(@ApiParam(value = "measure entity", required = true) @RequestBody Measure measure) {
        return measureService.createMeasure(measure);
    }
}
