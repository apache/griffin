# Apache Griffin Roadmap

## Current feature list
In the current version, we've implemented the below main DQ features

- **Data Asset Management**

  User can register, delete, edit data assets, currently only Hadoop data-sets are supported

- **Measure Management**

  User can create, delete, edit measures for 4 types: Accuracy, Profiling, Anomaly Detection, Publish Metrics

- **Job Scheduler**

  After the measures are created, the Job Scheduler component can create the jobs and schedule them to calculate the metrics values

- **Measure Execution on Spark**

  The Job Scheduler will trigger the measure execution on Spark to generate the metrics values

- **Metrics Visualization**

  We have a web portal to display all metrics

- **My Dashboard**

  Only the interested metrics will be displayed on "My Dashboard"


## Short-term Roadmap

- **Support more data-set types**  

  Current we only support Hadoop datasets, we should also support RDBMS and real-time streaming data from Kafka, Storm, etc.

- **Support more data quality dimensions**

  Besides accuracy, there are some other data quality dimensions(Completeness, Uniqueness, Timeliness, Validity, Consistency), we should support more dimensions

- **More ML algorithms for Anomaly Detection measure**

  Currently only [MAD(Median absolute deviation)](https://en.wikipedia.org/wiki/Median_absolute_deviation) and [Bollinger Bands](https://en.wikipedia.org/wiki/Bollinger_Bands) are supported, we are considering to support more Machine Learning algorithms
