## Docker webUI Guide

### Preparatory work

Follow the steps [here](https://github.com/apache/incubator-griffin#how-to-run-in-docker), prepare your docker container of griffin, and get your webUI ready.

### webUI test case guide

1.  Click "Data Assets" at the top right corner, to watch all the exist data assets.  
    In docker, we've prepared two data asset in Hive, through this page, you can see all the table metadata in Hive.

2.  Click "Measures" button at the top left corner to watch all the measures here, and you can also create a new DQ measurement by following steps.  
    1) Click "Create Measure" button at the top left corner, choose the top left block "Accuracy", at current we only support accuracy type.  
    2) Choose Source: find "demo_src" in the left tree, select some or all attributes in the right block, click "Next".  
    3) Choose Target: find "demo_tgt" in the left tree, select the matching attributes with source data asset in the right block, click "Next".  
    4) Mapping Source and Target: select "Source Fields" of each row, to match the corresponding field in target table, e.g. id maps to id, age maps to age, desc maps to desc.   
    Finish all the mapping, click "Next".  
    5) Fill out the required table as required, "Organization" is the group of this measurement.  
    Submit and save, you can see your new DQ measurement created in the measures list.  

3.  Now you've created a new DQ measurement, the measurement needs to be scheduled to run in the docker container. Click "Jobs" button to watch all the jobs here, at current there is no job, you need to create a new one.
    Click "Create Job" button at the top left corner, fill out all the blocks as below.
    ```
    "Source Partition": YYYYMMdd-HH
    "Target Partition": YYYYMMdd-HH
    "Measure Name": <choose the measure you just created>
    "Start After(s)": 0
    "Interval": 300
    ```
    The source and target partition means the partition pattern of the demo data, which is based on timestamp, "Start After(s)" means the job will start after n seconds, "Interval" is the interval of job, the unit is second. In the example above, the job will run every 5 minutes.

    Wait for about 1 minute, after the calculation, results would be published to web UI, then you can watch the dashboard by clicking "DQ Metrics" at the top right corner.
