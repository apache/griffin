# DQ Diagrams

## Entities

### DQMetric
> Represents a generic data quality metric used to assess various aspects of data quality (quantitative).

- **DQCompletenessMetric**
  > Measures the completeness of data, ensuring that all required data is present.

    - **DQCOUNTMetric**
      > A specific completeness metric that counts the number of non-missing values in a dataset.

- **DQAccuracyMetric**
  > Measures the accuracy of data, ensuring that data values are correct and conform to a known standard.

    - **DQNULLMetric**
      > An accuracy metric that counts the number of NULL values in a dataset.

- **DQUniquenessMetric**
  > Measures the uniqueness of data, ensuring that there are no duplicate records.

    - **DQUNIQUEMetric**
      > A specific uniqueness metric that identifies and counts unique records in a dataset.

- **DQFreshnessMetric**
  > Measures the freshness of data, ensuring that the data is up-to-date.

    - **DQTTUMetric (Time to Usable)**
      > A freshness metric that measures the time taken for data to become usable after it is created or updated.

- **DQDiffMetric**
  > Compares data across different datasets or points in time to identify discrepancies.

    - **DQTableDiffMetric**
      > A specific diff metric that compares entire tables to identify differences.

    - **DQFileDiffMetric**
      > A specific diff metric that compares files to identify differences.

- **MetricStorageService**
  > A data quality metric storage and fetch service


- **DQJob**
  > Abstract Data Quality related Jobs.
    - **MetricCollectingJob**
      > A job that collects data quality metrics from various sources and stores them for analysis.
 
    - **DQCheckJob**
      > A job that performs data quality checks based on predefined rules and metrics.
    
    - **DQAlertJob**
      > A job that generates alerts when data quality issues are detected.

    - **DQDag**
      > A directed acyclic graph that defines the dependencies and execution order of various data quality jobs.

- **Scheduler**
  > A system that schedules and manages the execution of data quality jobs. 
  > This is the default scheduler, it will launch data quality jobs periodically.

    - **DolphinSchdulerAdapter**
      > Connects our planed data quality jobs with Apache Dolphinscheduler,
      > allowing data quality jobs to be triggered upon the completion of dependent previous jobs.
    - **AirflowSchdulerAdapter**
      > Connects our planed data quality jobs with apache airflow,
      > so that data quality jobs can be triggered upon the completion of dependent previous jobs.