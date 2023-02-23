package org.apache.griffin.core.worker.entity.template;

import org.apache.griffin.core.worker.driver.TemplateDriver;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;

import java.util.Map;

public class DQRecordTemplate {

    private TemplateDriver driver;

    public String getRecordSql(Map<String, String> templateParams, DQEngineEnum engine) {
        return driver.getRecordSql(engine, this, templateParams);
    }
}
