## User Data Case Guide

Assuming that you've ran our docker example, here we will show you how to insert your own data and use griffin to calculate its data quality.  
As an example, our environment here is the docker container, which is built following [the docker guide](https://github.com/eBay/griffin/blob/master/README.md#how-to-run-in-docker), you can ssh into it and follow our steps. 

### Data Prepare

First of all, you might have got your initial data and schema. As we only support hive table now, you need to create a hive table.  

Here lists our [sample data](https://github.com/eBay/griffin/tree/master/docker/griffin-base/griffin/dataFile), we've put them into our docker image, 
and also created hive tables for them using [hql file](https://github.com/eBay/griffin/blob/master/docker/griffin-base/griffin/hive-input.hql).  

### Data Insert

Here we also use these two data files and their schemas but for new hive tables.
```
CREATE TABLE demo_source (
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

CREATE TABLE demo_target (
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
```
There will be new path in HDFS for data, you can put the data files directly using hdfs command or load them through hive.
```
LOAD DATA LOCAL INPATH 'dataFile/users_info_src.dat' OVERWRITE INTO TABLE demo_source;
LOAD DATA LOCAL INPATH 'dataFile/users_info_target.dat' OVERWRITE INTO TABLE demo_target;
```
Do not forget to touch a success file for your hdfs path.
```
hadoop fs -touchz /user/hive/warehouse/demo_source/_SUCCESS
hadoop fs -touchz /user/hive/warehouse/demo_target/_SUCCESS
```
Now we have inserted initial data, you can also modify or append data items later. Till now the data preparation is done.

### UI Operation

Switch to our UI part, we can follow [UI guide](https://github.com/eBay/griffin/blob/master/griffin-doc/dockerUIguide.md#webui-test-case-guide) to create our data assets inserted just now.  
You need to modify some items such as "Asset Name" and "HDFS Path" to your own ones set as above.  

After your model created, you just need to wait for the results, when your data is modified, the result would change at the next calculation.