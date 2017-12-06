package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.entity.Measure;
import org.apache.griffin.core.measure.entity.OutcomeMeasure;
import org.apache.griffin.core.metric.entity.MetricTemplate;
import org.apache.griffin.core.metric.repo.MetricTemplateRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MetricTemplateStoreImpl implements MetricTemplateStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricTemplateStoreImpl.class);

    @Autowired
    private MetricTemplateRepo templateRepo;

    @Override
    public void createTemplateFromMeasure(OutcomeMeasure measure) {
        if (templateRepo.findByCreatorTypeAndAndCreatorId("measure", measure.getId().toString()).size() != 0) {
            LOGGER.error("Failed to create metric template from measure {}, records already exist.", measure.getName());
        } else {
            saveTemplateFromMeasure(new MetricTemplate(), measure);
        }
    }

    @Override
    public void updateTemplateFromMeasure(OutcomeMeasure measure) {
        MetricTemplate template = getTemplateByCreator("measure", measure.getId().toString(), measure.getName());
        if (template != null) {
            saveTemplateFromMeasure(template, measure);
        }
    }

    @Override
    public void deleteTemplateFromMeasure(OutcomeMeasure measure) {
        MetricTemplate template = getTemplateByCreator("measure", measure.getId().toString(), measure.getName());
        if (template != null) {
            templateRepo.delete(template);
        }
    }

    @Override
    public void createTemplateFromJob(Measure measure, String jobId, String jobName) {
        List<MetricTemplate> templates = templateRepo.findByCreatorTypeAndAndCreatorId("job", jobId);
        if (templates.size() != 0) {
            LOGGER.error("Failed to create metric template from job {}, records already exist.", jobName);
        } else {
            MetricTemplate template = new MetricTemplate();
            template.setName(jobName);
            template.setCreatorType("job");
            template.setCreatorId(jobId);
            template.setMetricName(jobName);
            saveTemplate(template, measure);
        }
    }

    @Override
    public void deleteTemplateFromJob(String jobId, String jobName) {
        MetricTemplate template = getTemplateByCreator("job", jobId, jobName);
        if (template != null) {
            templateRepo.delete(template);
        }
    }

    private MetricTemplate getTemplateByCreator(String creatorType, String creatorId, String creatorName) {
        List<MetricTemplate> templates = templateRepo.findByCreatorTypeAndAndCreatorId(creatorType, creatorId);
        if (templates.size() == 0) {
            LOGGER.error("Metric template created by {} {} doesn't exist", creatorType, creatorName);
            return null;
        } else {
            return templates.get(0);
        }
    }

    private void saveTemplate(MetricTemplate template, Measure measure) {
        template.setDescription(measure.getDescription());
        template.setOrganization(measure.getOrganization());
        template.setOwner(measure.getOwner());
        try {
            templateRepo.save(template);
        } catch (Exception e) {
            LOGGER.error("Failed to save metric template. {}", e.getMessage());
        }
    }

    private void saveTemplateFromMeasure(MetricTemplate template, OutcomeMeasure measure) {
        template.setName(measure.getName());
        template.setCreatorType("measure");
        template.setCreatorId(measure.getId().toString());
        template.setMetricName(measure.getMetricName());
        saveTemplate(template, measure);
    }
}
