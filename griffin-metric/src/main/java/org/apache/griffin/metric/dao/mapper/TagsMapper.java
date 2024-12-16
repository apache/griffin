package org.apache.griffin.metric.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.griffin.metric.entity.TagAttachment;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface TagsMapper extends BaseMapper<TagAttachment> {
}
