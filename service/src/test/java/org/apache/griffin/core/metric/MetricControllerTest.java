package org.apache.griffin.core.metric;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@WebMvcTest(value=MetricController.class,secure = false)
public class MetricControllerTest {
    @Autowired
    private MockMvc mvc;

    @MockBean
    private MetricService service;

    @Before
    public void setup(){
    }


    @Test
    public void testGetOrgByMeasureName() throws IOException,Exception{

        given(service.getOrgByMeasureName("m14")).willReturn("bullseye");

        mvc.perform(get("/metrics/m14/org").contentType(MediaType.APPLICATION_JSON))
//                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isString())
                .andExpect(jsonPath("$",is("bullseye")))
        ;
    }

}
