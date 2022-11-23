package org.apache.griffin.core.worker.driver;

import org.apache.griffin.core.worker.entity.pojo.template.DQRecordTemplate;

import java.util.List;
import java.util.Map;

public class PrestoTemplateDriver extends TemplateDriver{
    @Override
    public List<String> getRecordSql(DQRecordTemplate template, Map<String, String> params) {
        return null;
    }
}
