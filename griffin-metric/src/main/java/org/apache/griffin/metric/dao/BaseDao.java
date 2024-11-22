/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.metric.dao;

import org.apache.commons.collections4.CollectionUtils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class BaseDao<E, M extends BaseMapper<E>> implements IDao<E> {

    protected M mybatisMapper;

    public BaseDao(@NonNull M mybatisMapper) {
        this.mybatisMapper = mybatisMapper;
    }

    @Override
    public E queryById(@NonNull Serializable id) {
        return mybatisMapper.selectById(id);
    }

    @Override
    public Optional<E> queryOptionalById(@NonNull Serializable id) {
        return Optional.ofNullable(queryById(id));
    }

    @Override
    public List<E> queryByIds(Collection<? extends Serializable> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        return mybatisMapper.selectBatchIds(ids);
    }

    @Override
    public List<E> queryAll() {
        return mybatisMapper.selectList(null);
    }

    @Override
    public List<E> queryByCondition(E queryCondition) {
        if (queryCondition == null) {
            throw new IllegalArgumentException("queryCondition can not be null.");
        }
        return mybatisMapper.selectList(new QueryWrapper<>(queryCondition));
    }

    @Override
    public int insert(@NonNull E entity) {
        return mybatisMapper.insert(entity);
    }

    @Override
    public void insertBatch(Collection<E> entities) {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }
        for (E e : entities) {
            insert(e);
        }
    }

    @Override
    public boolean updateById(@NonNull E entity) {
        return mybatisMapper.updateById(entity) > 0;
    }

    @Override
    public boolean deleteById(@NonNull Serializable id) {
        return mybatisMapper.deleteById(id) > 0;
    }

    @Override
    public boolean deleteByIds(Collection<? extends Serializable> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return true;
        }
        return mybatisMapper.deleteBatchIds(ids) > 0;
    }

    @Override
    public boolean deleteByCondition(E queryCondition) {
        if (queryCondition == null) {
            throw new IllegalArgumentException("queryCondition can not be null.");
        }
        return mybatisMapper.delete(new QueryWrapper<>(queryCondition)) > 0;
    }

}
