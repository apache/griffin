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


import org.apache.griffin.core.util.GriffinOperationMessage;
import org.springframework.web.bind.annotation.PathVariable;

public interface MeasureService {

    public Iterable<Measure> getAllMeasures();

    public Measure getMeasuresById(long id);

    public Measure getMeasuresByName(String measureName);


    public void deleteMeasuresById(@PathVariable("MeasureId") Long MeasureId);


    public GriffinOperationMessage deleteMeasuresByName(String measureName) ;

    public GriffinOperationMessage updateMeasure(Measure measure);


    public GriffinOperationMessage createNewMeasure(Measure measure);
}
