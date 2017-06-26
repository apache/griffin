<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
#Functional Specification Document

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

<table class="confluenceTable" border="2"><tbody><tr><th class="confluenceTh">#</th><th class="confluenceTh">Title</th><th class="confluenceTh">User Story</th><th class="confluenceTh">Importance</th><th colspan="1" class="confluenceTh">Status</th><th class="confluenceTh">Notes</th></tr><tr><td class="confluenceTd">1</td><td class="confluenceTd">User login</td><td class="confluenceTd"><p>User can login the system so that I'm able to access<br />the subsequent features.</p></td><td class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td class="confluenceTd"><ul><li>May use NT account, or a user list maintained in DB</li><li>Remember user for 1 month</li></ul></td></tr><tr><td class="confluenceTd">2</td><td class="confluenceTd">Create model - Accuracy</td><td class="confluenceTd"><p>User can create an accuracy model, so that the metrics</p><p>can be calculated by the rule defined in the model </p></td><td class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td class="confluenceTd"><ul><li>4 Steps in wizard:</li></ul><ol><li>Select the source dataset and fields which will be used for comparision</li><li>Select the target dataset and fields which will be used for comparision</li><li>Mapping the target fields with source, some basic functions should be <br />supported when mapping: exactly match,  length(), lower(), upper(), <br />trim(), etc.</li><li>Set basic configuration for your model: name, definition,threshold, <br />schedule type, email, etc.</li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, model list should be displayed and the new model is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">3</td><td colspan="1" class="confluenceTd"><span>Create model - Validity</span></td><td colspan="1" class="confluenceTd"><p><span>User can create a validity model, so that the metrics </span></p><p><span>can be </span><span>calculated by the rule defined in the model </span></p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>3 steps in wizard:</li></ul><ol><li>Select the target dataset and field which want to be checked, only one<br />field should be selected.</li><li>Define your syntax check logic which will be applied on the selected field,<br />including: <strong>Null Count, Unique Count, Duplicate Count, Maximum, </strong><br /><strong>Minimum, </strong><strong>Mean, Median, regular expression match count</strong></li><li><span>Set basic configuration for your model: name, definition,threshold, </span><br /><span>schedule type, email, etc.</span><span><br /></span></li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, model list should be displayed and the new model is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">4</td><td colspan="1" class="confluenceTd"><p><span>Create model - Anomaly</span></p><p><span>Detection</span></p></td><td colspan="1" class="confluenceTd"><p><span>User can create an anomaly model, so that the metrics </span></p><p><span>can be </span><span>calculated by the rule defined in the model </span></p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>3 steps in wizard:</li></ul><ol><li><span>Selected the predefined metrics of a data asset, such as &quot;Total Count&quot;,</span><br /><span>&quot;Null Count&quot;, or other validity metrics(calculated by validity model)</span></li><li><span>Select a statistical techniques: <strong>History Trend Detection, MAD, Bollinger</strong></span></li><li><span>Set basic configuration for your model: name, definition,threshold, </span><br /><span>schedule type, email, etc.</span></li></ol><ul><li>When submitting the form, need to confirm the form info</li><li>After submitting, model list should be displayed and the new model is <br />displayed as the 1st record.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">5</td><td colspan="1" class="confluenceTd"><p>Create model - Publish</p><p>Metrics</p></td><td colspan="1" class="confluenceTd"><p><span>User can create a &quot;publish metrics&quot; model, so that the </span></p><p><span>metrics can be </span><span>calculated by the rule defined in the model </span></p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Someone may already have their own metrics, so they only need to <br />manualy publish their metrics directly.</li><li>The publish URL and format should let the user know</li><li>User need to set basic configuration: name, definition,threshold, <br />schedule type, email, etc.</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">6</td><td colspan="1" class="confluenceTd">List models</td><td colspan="1" class="confluenceTd"><p>User can get a list of all models which contain basic model</p><p>definition info</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Information should be shown: Name, Org, Type, Desc, Create Date, Status</li><li>Sorted by &quot;Create Date&quot; desc</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">7</td><td colspan="1" class="confluenceTd">View a model</td><td colspan="1" class="confluenceTd">User can view the details of a model definition</td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>All the information should be displayed: name, definition, type, org, asset,<br />threshold, schedule type, email, owner, status, test result, daily status, etc.</li><li>Daily status can be a chart</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">8</td><td colspan="1" class="confluenceTd">Delete a model</td><td colspan="1" class="confluenceTd">User can delete my own models</td><td colspan="1" class="confluenceTd">May Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Administrator should also have delete permission</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">9</td><td colspan="1" class="confluenceTd">DataAsset list</td><td colspan="1" class="confluenceTd">User can view all data assets</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Information shown: name, type, HDFS path, org, create time, owner</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">10</td><td colspan="1" class="confluenceTd">Register DataAsset</td><td colspan="1" class="confluenceTd"><p>User can register his own data asset, so that he can</p><p>define models under this asset</p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Input these information: name, type(hdfs, hive or sql), path, folder pattern, <br /><strong>partition path</strong>, org, schema, owner</li><li>schema is a list, including name, type, desc, sample. We can also import <br />an existing schema from avro</li><li>owner is the logon user</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">11</td><td colspan="1" class="confluenceTd">Delete a DataAsset</td><td colspan="1" class="confluenceTd"><span>User can delete his own data assets</span></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"> </td></tr><tr><td colspan="1" class="confluenceTd">12</td><td colspan="1" class="confluenceTd">Edit a DataAsset</td><td colspan="1" class="confluenceTd"><span>User can edit his own data assets</span></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Name, type. org cannot be edited</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">13</td><td colspan="1" class="confluenceTd">Heatmap</td><td colspan="1" class="confluenceTd"><p>User can see the heatmap when I login, so that I'll know</p><p>the status of the metrics</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Below target, displayed as red; otherwise, green</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">14</td><td colspan="1" class="confluenceTd">Statistics of the system</td><td colspan="1" class="confluenceTd">User can see the statistics of the entire system</td><td colspan="1" class="confluenceTd">May Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>How many data assets totally</li><li>How many metrics totally</li><li>The healthy info of all metrics(the percentage of red/green)</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">15</td><td colspan="1" class="confluenceTd">List all metrics values</td><td colspan="1" class="confluenceTd">The application can provide the list of all metrics</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>List format: time, name, value.</li><li>Can be organized by Org-&gt;Asset-&gt;metrics list</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">16</td><td colspan="1" class="confluenceTd">Show metrics charts</td><td colspan="1" class="confluenceTd"><span>The application can display the charts of all metrics</span></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>A list of charts orgnized by <span>Org-&gt;Asset-&gt;metrics</span></li><li><span>Can filter by Org-&gt;Asset</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd">17</td><td colspan="1" class="confluenceTd">Show My Dashboard</td><td colspan="1" class="confluenceTd"><p>User can see &quot;my dashboard&quot;, so that he'll know the status</p><p>of his subscribed metrics</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>A list of charts</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">18</td><td colspan="1" class="confluenceTd">Subscribe metrics</td><td colspan="1" class="confluenceTd"><p>User can subscribe a metrics so that it can be displayed</p><p>on &quot;my dashboard&quot;</p></td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Subscribe steps</li></ul><ol><li>Select Org-&gt;Asset<ol><li>Can select on Org level, then all the assets under this Org will be <br />selected</li><li>Can select on Asset level</li><li>Can multiple select</li></ol></li><li>After the assets are selected, all the models under these assets should<br />be displayed, so that the end user will know which metrics he's selected</li><li>Do subscription</li></ol><p> </p></td></tr><tr><td colspan="1" class="confluenceTd">19</td><td colspan="1" class="confluenceTd">Edit subscription info</td><td colspan="1" class="confluenceTd">User can edit my subscription info</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>Can unsubscribe, and subscribe new metrics</li></ul></td></tr><tr><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">20</span></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">Define email notification</span></td><td colspan="1" class="confluenceTd"><p><span style="color: rgb(51,102,255);">User can request to send notification email when issues</span></p><p><span style="color: rgb(51,102,255);">detected</span></p></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">May Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li><span style="color: rgb(51,102,255);">On My Dashboard, I can specify a threshold for each metrics</span></li><li><span style="color: rgb(51,102,255);">Can define a unified email address or different email addresses for</span><br /><span style="color: rgb(51,102,255);">different metrics</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">21</span></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">Specify email report</span></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">User can decide the contents in email report</span></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">May Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li><span style="color: rgb(51,102,255);">User can select the metrics contained in the report, can also modify</span><br /><span style="color: rgb(51,102,255);">the selection</span></li><li><span style="color: rgb(51,102,255);">Specify email addresses</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">22</span></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">Report engine</span></td><td colspan="1" class="confluenceTd"><p><span style="color: rgb(51,102,255);">The application sends out the report regularly according </span></p><p><span style="color: rgb(51,102,255);">to user's subscription</span></p></td><td colspan="1" class="confluenceTd"><span style="color: rgb(51,102,255);">May Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li><span style="color: rgb(51,102,255);">Every day or every week, the report will be sent out at the same time</span></li><li><span style="color: rgb(51,102,255);">Main contents on the report are charts</span></li></ul></td></tr><tr><td colspan="1" class="confluenceTd">23</td><td colspan="1" class="confluenceTd">Scheduler job</td><td colspan="1" class="confluenceTd"><p>The application should have the scheduler jobs to</p><p>calculate the metrics</p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>According to the rules defined in the models, the scheduler job has these<br />functions:</li></ul><ol><li>Knows when to calculate a metrics</li><li>Call the model engine to get metrics values</li><li>Save the metrics result</li><li>Send notification emails if necessary</li></ol></td></tr><tr><td colspan="1" class="confluenceTd">24</td><td colspan="1" class="confluenceTd">Model engine</td><td colspan="1" class="confluenceTd">The application can calculate the metrics values</td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>With the rule definition, the engine can calculate the metrics values</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">25</td><td colspan="1" class="confluenceTd">Sampling</td><td colspan="1" class="confluenceTd"><p>User can download the sample data to analyze the</p><p>root cause when issue detected</p></td><td colspan="1" class="confluenceTd">Must Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"> </td></tr><tr><td colspan="1" class="confluenceTd">26</td><td colspan="1" class="confluenceTd">Profiling</td><td colspan="1" class="confluenceTd">User can profile the data</td><td colspan="1" class="confluenceTd"><span>Must Have</span></td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>According to the rules defined in the model, do data profiling</li></ul></td></tr><tr><td colspan="1" class="confluenceTd">27</td><td colspan="1" class="confluenceTd">User settings</td><td colspan="1" class="confluenceTd">User can specify my own settings</td><td colspan="1" class="confluenceTd">May Have</td><td colspan="1" class="confluenceTd"> </td><td colspan="1" class="confluenceTd"><ul><li>May set the unified email address for notifications</li><li>Set my default view: view the whole picture of all systems, or just my <br />concerned ones(defined in my dashboard)</li><li>......</li></ul></td></tr></tbody></table>

## User interaction and design

<p style="color: rgb(51,102,255);">#1 User Login</p>
<img src="img/fsd/image2016-6-30 13-19-2.png" >

<p style="color: rgb(51,102,255);">#2 Create model - Accuracy</p>
step 1 -> step 2 -> step 3 -> step 4 ->step 5 - confirmation    

<img src="img/fsd/image2016-6-30 16-16-21.png" >
<img src="img/fsd/image2016-6-30 16-17-5.png" >
<img src="img/fsd/image2016-6-30 16-17-52.png" >
<img src="img/fsd/image2016-6-30 16-18-20.png" >
<img src="img/fsd/image2016-6-30 16-18-52.png" >

<p style="color: rgb(51,102,255);">#3 Create model - Validity</p>
step 1 -> step 2 -> step 3 -> step 4 - confirmation     

<img src="img/fsd/image2016-6-30 16-20-34.png" >
<img src="img/fsd/image2016-6-30 16-20-53.png" >
<img src="img/fsd/image2016-6-30 16-21-16.png" >
<img src="img/fsd/image2016-6-30 16-21-49.png" >

<p style="color: rgb(51,102,255);">#4 Create model - Anomaly Detection</p>
step 1 -> step 2 -> step 3 -> step 4 - confirmation     

<img src="img/fsd/image2016-6-30 16-22-53.png" >
<img src="img/fsd/image2016-6-30 16-23-11.png" >
<img src="img/fsd/image2016-6-30 16-23-32.png" >
<img src="img/fsd/image2016-6-30 16-24-7.png" >

<p style="color: rgb(51,102,255);">#5 Create model - Publish Metrics</p>
step 1 -> step 2 - confirmation     

<img src="img/fsd/image2016-6-30 16-25-12.png" >
<img src="img/fsd/image2016-6-30 16-25-42.png" >

<p style="color: rgb(51,102,255);">#6 List models</p>
<img src="img/fsd/image2016-6-30 16-31-26.png" >

<p style="color: rgb(51,102,255);">#7 View a model</p>
<img src="img/fsd/image2016-6-30 16-33-44.png" >

<p style="color: rgb(51,102,255);">#8 Delete model</p>
<img src="img/fsd/image2016-6-30 16-34-58.png" >

<p style="color: rgb(51,102,255);">#9 DataAsset list</p>
<img src="img/fsd/image2016-6-30 16-35-18.png" >

<p style="color: rgb(51,102,255);">#10 Register DataAsset</p>
<i>&lt;Partition path isn't in this mock-up, will add it later &gt;</i>     

<img src="img/fsd/image2016-6-30 16-35-57.png" >
<img src="img/fsd/image2016-6-30 16-36-48.png" >

<p style="color: rgb(51,102,255);">#11 Delete a DataAsset</p>
<img src="img/fsd/image2016-6-30 16-39-17.png" >

<p style="color: rgb(51,102,255);">#12 Edit a DataAsset</p>
<img src="img/fsd/image2016-6-30 16-37-48.png" >

<p style="color: rgb(51,102,255);">#13 Heatmap</p>
<img src="img/fsd/image2016-6-30 16-39-37.png" >

<p style="color: rgb(51,102,255);">#14 Statistics of the system</p>
<img src="img/fsd/image2016-6-30 16-40-14.png" >

<p style="color: rgb(51,102,255);">#15 List all metrics values</p>
<img src="img/fsd/image2016-6-30 16-41-5.png" >

<p style="color: rgb(51,102,255);">#16 Show metrics charts</p>
chart list -> Max chart display     

<img src="img/fsd/image2016-6-30 16-41-57.png" >
<img src="img/fsd/image2016-6-30 16-42-16.png" >

<p style="color: rgb(51,102,255);">#17 Show My Dashboard</p>
<img src="img/fsd/image2016-6-30 16-44-15.png" >

<p style="color: rgb(51,102,255);">#18 Subscribe metrics</p>
TBD

<p style="color: rgb(51,102,255);">#19 Edit subscription info</p>
Same UI as #18

## Questions
Below is a list of questions to be addressed as a result of this requirements document:
<table border="2"><tbody><tr><th>Question</th><th>Outcome</th></tr></tbody></table>

## Not Doing
-
