package org.apache.griffin.core.measure.repo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.griffin.core.measure.DataConnector;
import org.apache.griffin.core.measure.EvaluateRule;
import org.apache.griffin.core.measure.Measure;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DataJpaTest
public class MeasureRepoTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private MeasureRepo measureRepo;

    @Before
    public void setup(){
        entityManager.clear();
        entityManager.flush();
    }

    @Test
    public void testFindAllOrganizations(){
        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = null;
        String configJson2 = null;
        try {
            configJson1 = new ObjectMapper().writeValueAsString(configMap1);
            configJson2 = new ObjectMapper().writeValueAsString(configMap2);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);

        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";

        EvaluateRule eRule = new EvaluateRule(1,rules);

        Measure measure = new Measure("m1","bevssoj description", Measure.MearuseType.accuracy, "bullseye", source, target, eRule,"owner1");
        entityManager.persistAndFlush(measure);

        DataConnector source2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule2 = new EvaluateRule(1,rules);
        Measure measure2 = new Measure("m2","test description", Measure.MearuseType.accuracy, "org1", source2, target2, eRule2,"owner1");
        entityManager.persistAndFlush(measure2);

        DataConnector source3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule3 = new EvaluateRule(1,rules);
        Measure measure3 = new Measure("m3","test_just_inthere description", Measure.MearuseType.accuracy, "org2", source3, target3, eRule3,"owner1");
        entityManager.persistAndFlush(measure3);


        List<String> orgs = measureRepo.findOrganizations();
        assertThat(orgs.size()).isEqualTo(3);

    }


    @Test
    public void testFindNameByOrganization(){
        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = null;
        String configJson2 = null;
        try {
            configJson1 = new ObjectMapper().writeValueAsString(configMap1);
            configJson2 = new ObjectMapper().writeValueAsString(configMap2);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);

        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";

        EvaluateRule eRule = new EvaluateRule(1,rules);

        Measure measure = new Measure("m1","bevssoj description", Measure.MearuseType.accuracy, "bullseye", source, target, eRule,"owner1");
        entityManager.persistAndFlush(measure);

        DataConnector source2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule2 = new EvaluateRule(1,rules);
        Measure measure2 = new Measure("m2","test description", Measure.MearuseType.accuracy, "org1", source2, target2, eRule2,"owner1");
        entityManager.persistAndFlush(measure2);

        DataConnector source3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule3 = new EvaluateRule(1,rules);
        Measure measure3 = new Measure("m3","test_just_inthere description", Measure.MearuseType.accuracy, "org2", source3, target3, eRule3,"owner1");
        entityManager.persistAndFlush(measure3);


        List<String> orgs = measureRepo.findNameByOrganization("org1");
        assertThat(orgs.size()).isEqualTo(1);
        assertThat(orgs.get(0)).isEqualToIgnoringCase("m2");

    }

    @Test
    public void testFindOrgByName(){
        HashMap<String,String> configMap1=new HashMap<>();
        configMap1.put("database","default");
        configMap1.put("table.name","test_data_src");
        HashMap<String,String> configMap2=new HashMap<>();
        configMap2.put("database","default");
        configMap2.put("table.name","test_data_tgt");
        String configJson1 = null;
        String configJson2 = null;
        try {
            configJson1 = new ObjectMapper().writeValueAsString(configMap1);
            configJson2 = new ObjectMapper().writeValueAsString(configMap2);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);

        String rules = "$source.uage > 100 AND $source.uid = $target.uid AND $source.uage + 12 = $target.uage + 10 + 2 AND $source.udes + 11 = $target.udes + 1 + 1";

        EvaluateRule eRule = new EvaluateRule(1,rules);

        Measure measure = new Measure("m1","bevssoj description", Measure.MearuseType.accuracy, "bullseye", source, target, eRule,"owner1");
        entityManager.persistAndFlush(measure);

        DataConnector source2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target2 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule2 = new EvaluateRule(1,rules);
        Measure measure2 = new Measure("m2","test description", Measure.MearuseType.accuracy, "org1", source2, target2, eRule2,"owner1");
        entityManager.persistAndFlush(measure2);

        DataConnector source3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson1);
        DataConnector target3 = new DataConnector(DataConnector.ConnectorType.HIVE, "1.2", configJson2);
        EvaluateRule eRule3 = new EvaluateRule(1,rules);
        Measure measure3 = new Measure("m3","test_just_inthere description", Measure.MearuseType.accuracy, "org2", source3, target3, eRule3,"owner1");
        entityManager.persistAndFlush(measure3);


        String org = measureRepo.findOrgByName("m3");
        assertThat(org).isEqualTo("org2");

    }
}
