CREATE TABLE metric_d (
                          metric_id BIGINT PRIMARY KEY,
                          metric_name VARCHAR(255) NOT NULL,
                          owner VARCHAR(255) NOT NULL,
                          description TEXT,
                          ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE metric_v (
                          id BIGINT AUTO_INCREMENT PRIMARY KEY,
                          metric_id BIGINT NOT NULL,
                          value DOUBLE NOT NULL,
                          ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                          FOREIGN KEY (metric_id) REFERENCES metric_d(metric_id)
);
CREATE TABLE tags (
                      id BIGINT AUTO_INCREMENT PRIMARY KEY,
                      metric_id BIGINT NOT NULL,
                      tag_key VARCHAR(255) NOT NULL,
                      tag_value VARCHAR(255) NOT NULL,
                      ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                      mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      FOREIGN KEY (metric_id) REFERENCES metric_d(metric_id)
);
