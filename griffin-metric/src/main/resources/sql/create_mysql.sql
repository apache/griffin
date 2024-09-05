-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS griffin;

-- Use the created database
USE griffin;

-- Create table for MetricD
DROP TABLE IF EXISTS `t_metric_d`;
CREATE TABLE IF NOT EXISTS t_metric_d (
    mid BIGINT AUTO_INCREMENT PRIMARY KEY,  -- Metric ID
    name VARCHAR(255) NOT NULL,             -- Metric name
    owner VARCHAR(255) NOT NULL,            -- Metric owner
    description TEXT,                       -- Metric description
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Creation time
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Modification time
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE = utf8_bin;

