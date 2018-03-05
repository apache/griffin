

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Functional Specification Document

## Goals

Apache Griffin is a model driven open source Data Quality solution for distributed data systems at any scale in both streaming or batch data context. When people use open source products (e.g. Hadoop, Spark, Kafka, Storm), they always need a data quality service to build his/her confidence on data quality processed by those platforms. Apache Griffin creates a unified process to define and construct data quality measurement pipeline across multiple data systems to provide:  

- Automatic quality validation of the data
- Data profiling and anomaly detection
- Data quality lineage from upstream to downstream data systems.
- Data quality health monitoring visualization
- Shared infrastructure resource management

## Background and strategic fit

At eBay, when people play with big data in Hadoop (or other streaming data), data quality often becomes one big challenge. Different teams have built customized data quality tools to detect and analyze data quality issues within their own domain. We are thinking to take a platform approach to provide shared Infrastructure and generic features to solve common data quality pain points. This would enable us to build trusted data assets.

Currently it’s very difficult and costly to do data quality validation when we have big data flow across multi-platforms at eBay (e.g. Oracle, Hadoop, Couchbase, Cassandra, Kafka, Mongodb). Take eBay real time personalization platform as a sample, everyday we have to validate data quality status for ~600M records ( imaging we have 150M active users for our website).  Data quality often becomes one big challenge both in its streaming and batch pipelines.

So we conclude 3 data quality problems at eBay:

1. Lack of end2end unified view of data quality measurement from multiple data sources to target applications, it usually takes a long time to identify and fix poor data quality.
2. How to get data quality measured in streaming mode, we need to have a process and tool to visualize data quality insights through registering dataset which you want to check data quality, creating data quality measurement model,  executing the data quality validation job and getting metrics insights for action taking.
3. No Shared platform and API Service, have to apply and manage own hardware and software infrastructure.

## Assumptions

- Users will primarily access this application from a PC
- We are handling textual data, no binary or encoded data

## Main business process

![Business_Process_image](img/Business_Process.png)

## Feature List

<table class="confluenceTable" border="2"><tbody><tr><th class="confluenceTh">#</th><th class="confluenceTh">Title</th><th class="confluenceTh">User Story</th><th class="confluenceTh">Importance</th><th colspan="1" class="confluenceTh">Status</th><th class="confluenceTh">Notes</th></tr><tr><td class="confluenceTd">1</td><td class="confluenceTd">User login</td><td class="confluenceTd"><p>User can login the system so that I'm able to access<br />the subsequent features.</p></td><td class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td class="confluenceTd"><ul><li>There are two login strategies you can choose</li><li>Ldap strategy: Use LDAP non-anonymous bind for user authentication</li><li>Default strategy: You can login with any username and password</li></ul></td></tr><tr><td class="confluenceTd">2</td><td class="confluenceTd">Create measure - Accuracy</td><td class="confluenceTd"><p>User can create an accuracy measure, so that the metrics can be calculated by the rule defined in the measure </p></td><td class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td class="confluenceTd"><ul><li>5  Steps in wizard:</li></ul><ol><li>Select the source dataset and fields which will be used for comparision</li><li>Select the target dataset and fields which will be used for comparision</li><li>Mapping the target fields with source, some basic operator should be supported when mapping:  =, !=, <,> etc.</li><li>Set partition configuration for source dataset and target dataset</li><li>Set basic configuration for your measure: name, description <br />measure type, etc.</li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, measure list should be displayed and the new measure is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">3</td><td colspan="1" class="confluenceTd"><span>Create measure -Data Profiling</span></td><td colspan="1" class="confluenceTd"><p><span>User can create a profiling measure, so that the metrics </span><span>can be </span><span>calculated by the rule defined in the measure </span></p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>4 steps in wizard:</li></ul><ol><li>Select the target dataset and field which want to be checked, only one field should be selected.</li><li>Define your syntax check logic which will be applied on the selected field,<br />including: <strong>Null Count,Distinct Count, Total Count, Maximum, </strong><br /><strong>Minimum, </strong><strong>Average, Enum Detection Top5 Count</strong></li><li>Set partition configuration for target dataset</li><li><span>Set basic configuration for your measure: name,description, </span><br /><span>etc.</span><span><br /></span></li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, measure list should be displayed and the new measure is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">4</td><td colspan="1" class="confluenceTd">List measures</td><td colspan="1" class="confluenceTd"><p>User can get a list of all measures which contain basic measure definition info</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Information should be shown: Measure Name,Measure Type, Description, Action</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">5</td><td colspan="1" class="confluenceTd">View a measure</td><td colspan="1" class="confluenceTd">User can view the details of a measure definition</td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>All the information should be displayed: measure name, description, measure type,<br />source,target,owner,etc.</li><li>Mapping rules and accuracy calculations formula are also displayed</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">6</td><td colspan="1" class="confluenceTd">Delete a measure</td><td colspan="1" class="confluenceTd">User can delete my own measures</td><td colspan="1" class="confluenceTd">May Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Administrator should also have delete permission</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">7</td><td colspan="1" class="confluenceTd"><p><span>Create Job</span></p></td><td colspan="1" class="confluenceTd"><p><span>User can create a job which schedules one measure you created </span></p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>1 step in wizard:</li></ul><ol><li><span>Set basic information for your job: measure name, cron expression, </span><span>data range, etc.</span></li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, job list should be displayed and the new job is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">8</td><td colspan="1" class="confluenceTd"><p>List jobs</p></td><td colspan="1" class="confluenceTd"><p>User can get a list of all jobs definition info</p><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Job information should be shown: Job Name,Previous Fire Time,Next Fire Time, Trigger State,Cron Expression, Action,Metric</li><li>Job instance information should be shown:AppID,Time,State</li><li>Jobinstance sorted by Time&quot; desc</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">9</td><td colspan="1" class="confluenceTd">Delete a job</td><td colspan="1" class="confluenceTd">User can delete my own jobs</td><td colspan="1" class="confluenceTd">May Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Administrator should also have delete permission</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">10</td><td colspan="1" class="confluenceTd">DataAsset list</td><td colspan="1" class="confluenceTd">User can view all data assets</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Information shown: table name, DB name, owner, creation time, location</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">11</td><td colspan="1" class="confluenceTd">Heatmap</td><td colspan="1" class="confluenceTd"><p>User can see the heatmap when I login, so that I'll know</p><p>the status of the metrics</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Show all metrics results displayed as green</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">12</td><td colspan="1" class="confluenceTd">Siderbar</td><td colspan="1" class="confluenceTd">The application can provide the list of all metrics</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>List format: time, name, value.</li><li>Can be organized by Measure-&gt;metrics list</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">13</td><td colspan="1" class="confluenceTd">Show mydashboard</td><td colspan="1" class="confluenceTd"><span>The application can display the charts of all metrics</span></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>A list of charts orgnized by <span>Measure-&gt;metrics list</span></li><li><span>Can filter by Measure</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd">14</td><td colspan="1" class="confluenceTd">Show metric detail</td><td colspan="1" class="confluenceTd"><span>Can display the detail of the metric</span></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li><span>Show metric details</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd">15</td><td colspan="1" class="confluenceTd">Scheduler job</td><td colspan="1" class="confluenceTd"><p>The application should have the scheduler jobs to</p><p>calculate the metrics</p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>According to the rules defined in the measures, the scheduler job has these<br />functions:</li></ul><ol><li>Know when to calculate a metric</li><li>Call the measure engine to get metrics values</li><li>Save the metrics result</li></ol></td></tr><tr><td colspan="1" class="confluenceTd">16</td><td colspan="1" class="confluenceTd">Measure engine</td><td colspan="1" class="confluenceTd">The application can calculate the metrics values</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>With the rule definition, the engine can calculate the metrics values</li></ul></td></tr></tbody></table>

## User interaction and design

<p style="color: rgb(51,102,255);">#1 User Login</p>

<img src="img/fsd/image2018-1-8 12-27-53.png" >

<p style="color: rgb(51,102,255);">#2 Create measure - Accuracy</p>
step 1 :  Select the source dataset and fields which will be used for comparision

<img src="img/fsd/image2018-1-8 12-33-2.png" >

step 2  :  Select the target dataset and fields which will be used for comparision

<img src="img/fsd/image2018-1-8 12-38-12.png" >

step 3 :  Mapping the target fields with source

<img src="img/fsd/image2018-1-8 12-44-30.png" >

step 4  :  Set partition configuration for source dataset and target dataset

<img src="img/fsd/image2018-1-8 12-48-20.png" >

step 5  :  Set basic configuration for your measure

<img src="img/fsd/image2018-1-8 12-51-13.png" >

confirmation  : 

<img src="img/fsd/image2018-1-8 13-02-15.png" >

<p style="color: rgb(51,102,255);">#3 Create measure - Profiling</p>
step 1 :  Select the target dataset and field which want to be checked

<img src="img/fsd/image2018-1-8 13-07-16.png" >

step 2  :  Define your syntax check logic which will be applied on the selected field

<img src="img/fsd/image2018-1-8 13-10-28.png" >

step 3  :  Set partition configuration for target dataset

<img src="img/fsd/image2018-1-8 13-12-10.png" >

step 4  :  Set basic configuration for your measure

<img src="img/fsd/image2018-1-8 13-13-40.png" >

confirmation   :

<img src="img/fsd/image2018-1-8 13-15-30.png" >

<p style="color: rgb(51,102,255);">#4 List measures</p>
<img src="img/fsd/image2018-2-5 14-45-10.png" >

<p style="color: rgb(51,102,255);">#5 View a measure</p>
<img src="img/fsd/image2018-2-5 14-44-20.png" >

<p style="color: rgb(51,102,255);">#6 Delete a measure</p>
<img src="img/fsd/image2018-2-5 14-46-26.png" >

<p style="color: rgb(51,102,255);">#7 Create job</p>

<img src="img/fsd/image2018-2-6 11-1-10.png" >

<img src="img/fsd/image2018-2-6 11-3-19.png" >

<p style="color: rgb(51,102,255);">#8 List jobs</p>

<img src="img/fsd/image2018-2-6 10-54-15.png" >

<img src="img/fsd/image2018-2-6 10-57-20.png" >

<p style="color: rgb(51,102,255);">#9 Delete a job</p>

<img src="img/fsd/image2018-2-6 10-58-24.png" >

<p style="color: rgb(51,102,255);">#10 DataAsset list</p>
<img src="img/fsd/image2018-2-5 14-50-10.png" >

<p style="color: rgb(51,102,255);">#11 Heatmap</p>
<img src="img/fsd/image2018-2-6 10-10-15.png" >

<p style="color: rgb(51,102,255);">#12 List all metrics values</p>
<img src="img/fsd/image2018-2-6 10-48-10.png" height="500px">



<p style="color: rgb(51,102,255);">#13 Show My Dashboard</p>
chart list -> Max chart display     

<img src="img/fsd/image2018-2-6 10-50-10.png" height="400px" >

<p style="color: rgb(51,102,255);">#14 Show metric detail</p>
<img src="img/fsd/image2018-2-6 10-51-16.png" height="400px" >

<p style="color: rgb(51,102,255);">#15 Scheduler job</p>
Same UI as #7

## Questions
Below is a list of questions to be addressed as a result of this requirements document:
<table border="2"><tbody><tr><th>Question</th><th>Outcome</th></tr></tbody></table>

## Not Doing
-
