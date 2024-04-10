package org.apache.griffin.common.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * Base entity.
 *
 */
@Getter
@Setter
public abstract class BaseEntity implements java.io.Serializable {
    private static final long serialVersionUID = -7150065349727498445L;

    private Long id;

    protected Date updatedAt;

    protected Date createdAt;

}
