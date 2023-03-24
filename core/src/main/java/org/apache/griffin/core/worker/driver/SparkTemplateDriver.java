package org.apache.griffin.core.worker.driver;

import org.apache.griffin.core.worker.entity.template.DQRecordTemplate;

import java.util.Map;

public class SparkTemplateDriver extends TemplateDriver {
    @Override
    public String getRecordSql(DQRecordTemplate template, Map<String, String> params) {
        return null;
    }
}
