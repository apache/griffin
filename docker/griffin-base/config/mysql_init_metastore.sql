CREATE DATABASE metastore;
USE metastore;
SOURCE /apache/hive/scripts/metastore/upgrade/mysql/hive-schema-1.2.0.mysql.sql;


CREATE USER 'hive'@'localhost' IDENTIFIED BY '123456';
REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'hive'@'localhost';
GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES,EXECUTE ON metastore.* TO 'hive'@'localhost';
FLUSH PRIVILEGES;
