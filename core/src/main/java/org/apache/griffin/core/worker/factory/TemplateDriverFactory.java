package org.apache.griffin.core.worker.factory;

import com.google.common.collect.Maps;
import org.apache.griffin.core.worker.driver.PrestoTemplateDriver;
import org.apache.griffin.core.worker.driver.SparkTemplateDriver;
import org.apache.griffin.core.worker.driver.TemplateDriver;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class TemplateDriverFactory {
    Map<DQEngineEnum, TemplateDriver> engineDriverMap = Maps.newConcurrentMap();

    public TemplateDriver getTemplateDrvier(DQEngineEnum engine) {
        TemplateDriver templateDriver = engineDriverMap.get(engine);
        if (templateDriver != null) return templateDriver;

        switch (engine) {
            case PRESTO:
                templateDriver =  new PrestoTemplateDriver();
                break;
            case SPARK:
                templateDriver = new SparkTemplateDriver();
                break;
            default:
                break;
        }
        engineDriverMap.put(engine, templateDriver);
        return templateDriver;
    }
}
