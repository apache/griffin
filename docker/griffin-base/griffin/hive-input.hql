CREATE TABLE users_info_src (
  user_id bigint,
  first_name string,
  last_name string,
  address string,
  email string,
  phone string,
  post_code string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;



CREATE TABLE users_info_target (
  user_id bigint,
  first_name string,
  last_name string,
  address string,
  email string,
  phone string,
  post_code string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
