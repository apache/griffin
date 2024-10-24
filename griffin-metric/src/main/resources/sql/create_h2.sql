DROP TABLE IF EXISTS t_metric_d;
CREATE TABLE t_metric_d (
    mid BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    description TEXT,
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS t_tag_d;
CREATE TABLE t_tag_d (
    tid BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    val DOUBLE NOT NULL,
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS t_metric_tag;
CREATE TABLE t_metric_tag (
    mid BIGINT references t_metric_d(mid),
    tid BIGINT references t_tag_d(tid),
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(tid,mid)
);

DROP TABLE IF EXISTS t_metric_v;
CREATE TABLE t_metric_v (
    mid BIGINT PRIMARY KEY references t_metric_d(mid),
    val DOUBLE NOT NULL,
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
