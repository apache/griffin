package org.apache.griffin.core.metric;

import org.apache.griffin.core.measure.entity.ExternalMeasure;
import org.apache.griffin.core.measure.entity.Measure;
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
    public Boolean createFromMeasure(ExternalMeasure measure) {
        if (templateRepo.findByCreatorTypeAndCreatorId(MetricTemplate.CreatorType.MEASURE, measure.getName()).size() != 0) {
            LOGGER.error("Failed to create metric template from measure {}, records already exist.", measure.getName());
            return false;
        } else {
            return saveFromMeasure(new MetricTemplate(), measure);
        }
    }

    @Override
    public Boolean updateFromMeasure(ExternalMeasure measure) {
        MetricTemplate template = getByCreator(MetricTemplate.CreatorType.MEASURE, measure.getName());
        if (template == null) {
            return false;
        } else {
            return saveFromMeasure(template, measure);
        }
    }

    @Override
    public Boolean deleteFromMeasure(ExternalMeasure measure) {
        MetricTemplate template = getByCreator(MetricTemplate.CreatorType.MEASURE, measure.getName());
        if (template == null) {
            return false;
        } else {
            templateRepo.delete(template);
            return true;
        }
    }

    @Override
    public Boolean createFromJob(Measure measure, String jobId, String jobName) {
        List<MetricTemplate> templates = templateRepo.findByCreatorTypeAndCreatorId(MetricTemplate.CreatorType.JOB, jobId);
        if (templates.size() != 0) {
            LOGGER.error("Failed to create metric template from job {}, records already exist.", jobName);
            return false;
        } else {
            MetricTemplate template = new MetricTemplate();
            template.setName(jobName);
            template.setCreatorType(MetricTemplate.CreatorType.JOB);
            template.setCreatorId(jobId);
            template.setMetricName(jobName);
            return save(template, measure);
        }
    }

    @Override
    public Boolean deleteFromJob(String jobId, String jobName) {
        MetricTemplate template = getByCreator(MetricTemplate.CreatorType.JOB, jobId);
        if (template == null) {
            return false;
        } else {
            templateRepo.delete(template);
            return true;
        }
    }

    private MetricTemplate getByCreator(MetricTemplate.CreatorType creatorType, String creatorId) {
        List<MetricTemplate> templates = templateRepo.findByCreatorTypeAndCreatorId(creatorType, creatorId);
        if (templates.size() == 0) {
            LOGGER.error("Metric template created by {} {} doesn't exist", creatorType, creatorId);
            return null;
        } else {
            return templates.get(0);
        }
    }

    private Boolean saveFromMeasure(MetricTemplate template, ExternalMeasure measure) {
        template.setName(measure.getName());
        template.setCreatorType(MetricTemplate.CreatorType.MEASURE);
        template.setCreatorId(measure.getName());
        template.setMetricName(measure.getMetricName());
        return save(template, measure);
    }

    private Boolean save(MetricTemplate template, Measure measure) {
        template.setDescription(measure.getDescription());
        template.setOrganization(measure.getOrganization());
        template.setOwner(measure.getOwner());
        try {
            if (templateRepo.save(template) != null) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to save metric template. {}", e.getMessage());
        }
        return false;
    }
}
