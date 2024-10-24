package org.apache.griffin.metric.dao;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.griffin.metric.dao.mapper.TagsMapper;
import org.apache.griffin.metric.entity.TagAttachment;
import org.springframework.stereotype.Repository;

import java.util.Objects;

@Repository
@Slf4j
public class TagAttachmentDao extends BaseDao<TagAttachment, TagsMapper> {

    public TagAttachmentDao(@NonNull TagsMapper mybatisMapper) {
        super(mybatisMapper);
    }

    public int addMetricTags(TagAttachment tagAttachment) {
        if (Objects.isNull(tagAttachment)) {
            log.warn("tags is invalid");
            return 0;
        }

        if (Objects.isNull(tagAttachment.getMetricId())) {
            log.warn("metric id is invalid");
            return 0;
        }

        TagAttachment existence = mybatisMapper.selectById(tagAttachment.getMetricId());
        if (Objects.nonNull(existence)) {
            log.warn("tagAttachment has existed");
            return 0;
        }

        int count = mybatisMapper.insert(tagAttachment);
        log.info("add tags: {}, count: {}", tagAttachment, count);
        return count;
    }
}
