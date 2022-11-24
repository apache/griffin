package org.apache.griffin.core.worker.entity.dispatcher;

import lombok.Data;
import org.apache.griffin.core.worker.entity.enums.DQEngineEnum;

@Data
public class SubmitRequest {
    private String recordSql;
    private DQEngineEnum engine;  // Spark,Hive,Presto,etc.
    private String owner;
    private Integer maxRetryCount = 3;

    public SubmitRequest() {
    }

    public SubmitRequest(String recordSql, DQEngineEnum engine, String owner, Integer maxRetryCount) {
        this.recordSql = recordSql;
        this.engine = engine;
        this.owner = owner;
        this.maxRetryCount = maxRetryCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String recordSql;
        private DQEngineEnum engine;
        private String owner;
        private Integer maxRetryCount = 3;


        public Builder recordSql(String recordSql) {
            this.recordSql = recordSql;
            return this;
        }

        public Builder engine(DQEngineEnum engine) {
            this.engine = engine;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder retryCount(Integer maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public SubmitRequest build() {
            return new SubmitRequest(this.recordSql, this.engine, this.owner, this.maxRetryCount);
        }
    }

}
