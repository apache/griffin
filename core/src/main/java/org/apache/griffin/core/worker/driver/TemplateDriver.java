package org.apache.griffin.core.worker.driver;

import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.apache.griffin.core.worker.entity.template.DQRecordTemplate;
import org.apache.griffin.core.worker.factory.TemplateDriverFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public abstract class TemplateDriver {

    @Autowired
    private TemplateDriverFactory templateDriverFactory;

    /**
     * 拼出的SQL 返回值 必须是 <ruleId, Partition, Metric>  这样可以做SQL合并
     */
    public abstract String getRecordSql(DQRecordTemplate template, Map<String, String> params);

    public String getRecordSql(DQEngineEnum engine, DQRecordTemplate template, Map<String, String> params) {
        TemplateDriver templateDriver = templateDriverFactory.getTemplateDrvier(engine);
        return templateDriver.getRecordSql(template, params);
    }

}
