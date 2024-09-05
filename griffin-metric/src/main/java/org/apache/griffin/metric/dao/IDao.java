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

import lombok.NonNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface IDao<E> {

    /**
     * Query by the primary key.
     */
    E queryById(@NonNull Serializable id);

    Optional<E> queryOptionalById(@NonNull Serializable id);

    List<E> queryByIds(Collection<? extends Serializable> ids);

    List<E> queryAll();

    List<E> queryByCondition(E queryCondition);

    int insert(@NonNull E model);

    void insertBatch(Collection<E> entities);

    boolean updateById(@NonNull E entity);

    boolean deleteById(@NonNull Serializable id);

    boolean deleteByIds(Collection<? extends Serializable> ids);

    boolean deleteByCondition(E queryCondition);

}
