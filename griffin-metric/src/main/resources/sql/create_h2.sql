CREATE TABLE t_metric_d (
    mid BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    description TEXT,
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE t_tag_d (
    mid BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    val DOUBLE NOT NULL
);

CREATE TABLE t_metric_tag (
    tid BIGINT PRIMARY KEY references t_metric_d(mid),
    mtid BIGINT references t_tag_d(mid)
);

CREATE TABLE t_metric_v (
    mid BIGINT PRIMARY KEY references t_metric_d(mid),
    val DOUBLE NOT NULL,
    tid BIGINT references t_metric_tag(tid),
    ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
