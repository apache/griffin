CREATE DATABASE metastore;
USE metastore;
SOURCE /apache/hive/scripts/metastore/upgrade/mysql/hive-schema-1.2.0.mysql.sql;

CREATE USER 'hive'@'%' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
