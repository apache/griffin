package com.ebay.oss.griffin.service;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.ebay.oss.griffin.domain.DqModel;
import com.ebay.oss.griffin.repo.DqModelRepo;
import com.ebay.oss.griffin.service.RefMetricsCalcImpl;


public class RefMetricsCalcImplTest {

    private RefMetricsCalcImpl calc;

    @Before
    public void setUp() {
        calc = new RefMetricsCalcImpl();
    }

    @SuppressWarnings("serial")
    @Test
    public void test_getReferences() {
        calc.modelRepo = mock(DqModelRepo.class);
        
        List<DqModel> models = new ArrayList<>();
        models.add(newModelWithRef("model1", "ref1"));
        models.add(newModelWithRef("model2", "ref2_0, ref2_1, ref2_2"));
        models.add(newModelWithRef("model2", null));
        models.add(newModelWithRef("model2", ""));
        when(calc.modelRepo.getAll()).thenReturn(models);
        
        Map<String, List<String>> expect = new HashMap<String, List<String>>() {{
            this.put("model1", new ArrayList<String>() {{
                this.add("ref1");
            }});
            this.put("model2", new ArrayList<String>() {{
              this.add("ref2_0");
              this.add("ref2_1");
              this.add("ref2_2");  
            }});
        }};
        
        Map<String, List<String>> actual = calc.getReferences();

        assertEquals(expect, actual);

        verify(calc.modelRepo).getAll();
    }

    private DqModel newModelWithRef(String name, String ref) {
        DqModel model = new DqModel();
        model.setModelName(name);
        model.setReferenceModel(ref);
        return model;
    }

}
