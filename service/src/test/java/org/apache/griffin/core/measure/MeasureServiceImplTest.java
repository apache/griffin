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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
public class MeasureServiceImplTest {

    @TestConfiguration
    public static class HiveMetastoreServiceConfiguration{
        @Bean
        public MeasureServiceImpl service(){
            return new MeasureServiceImpl();
        }
    }
    @MockBean
    private MeasureRepo measureRepo;

    @Autowired
    private MeasureServiceImpl service;

    @Before
    public void setup(){

    }

    @Test
    public void testGetAllMeasures(){
        try {
            Iterable<Measure> tmp = service.getAllMeasures();
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all Measure from dbs");
        }
    }

    @Test
    public void testGetMeasuresById(){
        try {
            Measure tmp = service.getMeasuresById(1);
            assertTrue(true);
        }catch (Throwable t){
            fail("Cannot get all tables in db default");
        }
    }

}
