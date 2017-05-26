package org.apache.griffin.core.metric;


import org.apache.griffin.core.measure.repo.MeasureRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetricServiceImpl implements MetricService{
    @Autowired
    MeasureRepo measureRepo;
    @Override
    public String getOrgByMeasureName(String measureName) {
        return measureRepo.findOrgByName(measureName);
    }
}
