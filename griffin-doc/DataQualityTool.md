# Data Quality Tool

## Introduction

In the evolving landscape of data architecture, ensuring data quality remains a critical success factor for all companies.
Data architectures have progressed significantly over recent years, transitioning from relational databases and data
warehouses to data lakes, hybrid data lake and warehouse combinations, and modern lakehouses.

Despite these advancements, data quality issues persist and have become increasingly vital, especially in the era of AI
and data integration. Improving data quality is essential for all organizations, and maintaining it across various
environments requires a combination of people, processes, and technology.

To address these challenges, we will upgrade data quality tool designed to be easily adopted by any data organization.
This tool abstracts common data quality problems and integrates seamlessly with diverse data architectures.

## Data Quality Dimensions

1. **Accuracy** – Data should be error-free by business needs.
2. **Consistency** – Data should not conflict with other values across data sets.
3. **Completeness** – Data should not be missing.
4. **Timeliness** – Data should be up-to-date in a limited time frame
5. **Uniqueness** – Data should have no duplicates.
6. **Validity** – Data should conform to a specified format.

## Our new Architecture

Our new architecture consists of two primary layers: the Data Quality Layer and the Integration Layer.

### Data Quality Constraints Layer

This constraints layer abstracts the core concepts of the data quality lifecycle, focusing on:

- **Defining Specific Data Quality Constraints**:
  - **Metrics**: Establishing specific data quality metrics.
  - **Anomaly Detection**: Implementing methods for detecting anomalies.
  - **Actions**: Defining actions to be taken based on the data quality assessments.

- **Measuring Data Quality**:
  - Utilizing various connectors such as SQL, HTTP, and CMD to measure data quality across different systems.

- **Unifying Data Quality Results**:
  - Creating a standardized and structured view of data quality results across different dimensions to ensure a consistent understanding.

- **Flexible Data Quality Jobs**:
  - Designing data quality jobs within a generic, topological Directed Acyclic Graph (DAG) framework to facilitate easy plug-and-play functionality.

### Integration Layer

This layer provides a robust framework to enable users to integrate Griffin data quality pipelines seamlessly with their business processes. It includes:

- **Scheduler Integration**:
  - Ensuring seamless integration with typical schedulers for efficient pipeline execution.

- **Apache DolphinScheduler Integration**:
  - Facilitating effortless integration within the Java ecosystem to leverage Apache DolphinScheduler.

- **Apache Airflow Integration**:
  - Enabling smooth integration within the AI ecosystem using Apache Airflow.

This architecture aims to provide a comprehensive and flexible approach to managing data quality
and integrating it into various existing business workflows in data team.

So that enterprise job scheduling system will launch optional data quality check pipelines after usual data jobs are finished.
And maybe based on data quality result, schedule some actions such as retry or stop the downstream scheduling like circuit breaker.

### Data Quality Layer

#### Data Quality Constraints Definition

This concept has been thoroughly discussed in the original Apache Griffin design documents. Essentially, we aim to quantify
the data quality of a dataset based on the aforementioned dimensions. For example, to measure the count of records in a user
table, our data quality constraint definition could be:

**Simple Version:**

- **Metric**
  - Name: count_of_users
  - Target: user_table
  - Dimension: count
- **Anomaly Condition:** $metric <= 0
- **Post Action:** send alert

**Advanced Version:**

- **Metric**
  - Name: count_of_users
  - Target: user_table
  - Filters: city = 'shanghai' and event_date = '20240601'
  - Dimension: count
- **Anomaly Condition:** $metric <= 0
- **Post Action:** send alert

#### Data Quality Pipelines(DAG)

We support several typical data quality pipelines:

**One Dataset Profiling Pipeline:**

```plaintext
recording_target_table_metric_job -> anomaly_condition_job -> post_action_job
```

**Dataset Diff Pipeline:**

```plaintext
recording_target_table1_metric_job  ->
                                       \
                                        -> anomaly_condition_job  -> post_action_job
                                       /
recording_target_table2_metric_job  ->
```

**Compute Platform Migration Pipeline:**

```plaintext
run_job_on_platform_v1 -> recording_target_table_metric_job_on_v1  ->
                                                                       \
                                                                        -> anomaly_condition_job  -> post_action_job
                                                                       /
run_job_on_platform_v2 -> recording_target_table_metric_job_on_v2  ->
```
#### Data Quality Report

- **Meet Expectations**
  + Data Quality Constrain 1: Passed
  + Data Quality Constrain 2: Passed
- **Does Not Meet Expectations**
  + Data Quality Constrain 3: Failed
    - Violation details
    - Possible root cause 
  + Data Quality Constrain 4: Failed
    - Violation details
    - Possible root cause

#### Connectors

The executor measures the data quality of the target dataset by recording the metrics. It supports many predefined protocols,
and customers can extend the executor protocol if they want to add their own business logic.

**Predefined Protocols:**

- MySQL: `jdbc:mysql://hostname:port/database_name?user=username&password=password`
- Presto: `jdbc:presto://hostname:port/catalog/schema`
- Trino: `jdbc:trino://hostname:port/catalog/schema`
- HTTP: `http://hostname:port/api/v1/query?query=<prometheus_query>`
- Docker

### Integration layer

Every data team has its own existing scheduler.
While we provide a default scheduler, for greater adoption, we will refactor
our Apache Griffin scheduler capabilities to leverage our customers' schedulers.
This involves redesigning our scheduler to either ingest job instances into our customers' schedulers
or bridge our DQ pipelines to their DAGs.

```plaintext
  biz_etl_phase  ||     data_quality_phase
                 ||
business_etl_job -> recording_target_table1_metric_job  - ->
                 ||                                         \
                 ||                                           -> anomaly_condition_job  -> post_action_job
                 ||                                         /
business_etl_job -> recording_target_table2_metric_job  - ->
                 ||
```

 - integration with a generic scheduler

 - integration with apache dolphinscheduler

 - integration with apache airflow






