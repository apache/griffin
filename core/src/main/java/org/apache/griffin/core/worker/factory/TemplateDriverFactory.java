package org.apache.griffin.core.worker.factory;

import org.apache.griffin.core.worker.driver.TemplateDriver;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;
import org.springframework.stereotype.Service;

@Service
public class TemplateDriverFactory {
    public TemplateDriver getTemplateDrvier(DQEngineEnum engine) {
        return null;
    }
}
