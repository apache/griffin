package org.apache.griffin.core.measure;//package org.apache.griffin.core.measure;
//
//import org.apache.griffin.core.measure.entity.DataConnector;
//import org.apache.griffin.core.measure.entity.EvaluateRule;
//import org.apache.griffin.core.measure.entity.Measure;
//import org.apache.griffin.core.measure.repo.DataConnectorRepo;
//import org.apache.griffin.core.measure.repo.EvaluateRuleRepo;
//import org.apache.griffin.core.measure.repo.MeasureRepo;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
//import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
//import org.springframework.context.annotation.PropertySource;
//import org.springframework.test.context.jdbc.Sql;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.transaction.annotation.Propagation;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//@RunWith(SpringRunner.class)
//@PropertySource("classpath:application.properties")
//@DataJpaTest
////@Sql(value = {"classpath:Init_quartz.sql", "classpath:quartz-test.sql"})
//@Sql("classpath:test.sql")
//public class MeasureRepoTest {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(MeasureRepoTest.class);
//
//
//    @Autowired
//    private TestEntityManager testEntityManager;
//
//    @Autowired
//    private MeasureRepo measureRepo;
//    @Autowired
//    private DataConnectorRepo dataConnectorRepo;
//    @Autowired
//    private EvaluateRuleRepo evaluateRuleRepo;
//
//    @Test
//    public void testFindOrganizations() {
//        Iterable<String> orgs = measureRepo.findOrganizations();
//        System.out.println(orgs);
//        for (String org : orgs) {
//            assertThat(org).isEqualTo("eBay");
//        }
//
//    }
//
//    @Test
//    public void testFindNameByOrganization() {
//        List<String> names = measureRepo.findNameByOrganization("eBay");
//        assertThat(names.get(0)).isEqualTo("buy_rates_hourly");
//        assertThat(names.get(1)).isEqualTo("griffin_aver");
//    }
//
//    @Test
//    public void testFindOrgByName() {
//        assertThat(measureRepo.findOrgByName("buy_rates_hourly")).isEqualTo("eBay");
//        assertThat(measureRepo.findOrgByName("griffin_aver")).isEqualTo("eBay");
//    }
//
//   /* @Test
//    @Transactional(propagation = Propagation.NOT_SUPPORTED)
//    public void testUpdateMeasure() {
//        HashMap<String, String> sourceMap = new HashMap<>();
//        sourceMap.put("database", "griffin");
//        sourceMap.put("table.name", "count");
//        DataConnector source = new DataConnector(DataConnector.ConnectorType.HIVE, "1.3", sourceMap);
//        HashMap<String, String> targetMap = new HashMap<>();
//        targetMap.put("database", "default");
//        targetMap.put("table.name", "avr_in");
//        DataConnector target = null;
//        try {
//            target = new DataConnector(DataConnector.ConnectorType.HIVE, "1.4", new ObjectMapper().writeValueAsString(targetMap));
//        } catch (IOException e) {
//            LOGGER.error("Fail to convert map to string using ObjectMapper.");
//        }
//
//        EvaluateRule rule = new EvaluateRule(0, "$source['uid'] == $target['url'] AND $source['uage'] == $target['createdts']");
//        //save before flushing
//        dataConnectorRepo.save(source);
//        dataConnectorRepo.save(target);
//        evaluateRuleRepo.save(rule);
//        measureRepo.updateMeasure((long) 1, "new desc2", "Paypal", source, target, rule);
//        for (Measure measure : measureRepo.findAll()) {
//            if (measure.getId().equals((long) 1)) {
//                assertThat(measure.getDescription()).isEqualTo("new desc2");
//                assertThat(measure.getOrganization()).isEqualTo("Paypal");
//                assertThat(measure.getSource()).isEqualTo(source);
//                assertThat(measure.getTarget()).isEqualTo(target);
//                assertThat(measure.getEvaluateRule()).isEqualTo(rule);
//            }
//        }
//
//    }*/
//}
