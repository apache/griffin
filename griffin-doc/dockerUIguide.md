## Docker webUI Guide

### Preparatory work

Follow the steps [here](https://github.com/eBay/DQSolution/blob/master/README.md#how-to-run-in-docker), prepare your docker container of griffin, and get your webUI ready.

### webUI test case guide

1.  Click "Data Assets" at the top right corner, to watch all the exist data assets.

2.  Click "Register Data Asset" button at the top left corner, fill out the "Required Information" table as the following data, then submit and save to finish the creation of a new data asset.
    ```
    Asset Name:     users_info_src
    Asset Type:     hivetable
    HDFS Path:      /user/hive/warehouse/users_info_src
    Organization:   <any>
    Schema:         user_id     bigint
                    first_name  string
                    last_name   string
                    address     string
                    email       string
                    phone       string
                    post_code   string
    ```
    The data asset "users_info_src" has been prepared in our docker image already, and the information is shown above.  
    "Asset Name" item needs to be the same with the Hive table name, "HDFS Path" is exactly the path in HDFS, they should be filled as real.  
    "Asset Type" item has only one selection "hivetable" at current, and you need to choose it, while the "Organization" item could be set as any one you like.  
    "Schema" item lists the schema of the data asset, the names and types are better to be exactly the same with the hive table while it's not required now, but the number and order of schema items need to be.  

    Repeat the above step, create another new data asset by filling out as following.
    ```
    Asset Name:     users_info_target
    Asset Type:     hivetable
    HDFS Path:      /user/hive/warehouse/users_info_target
    Organization:   <any>
    Schema:         user_id     bigint
                    first_name  string
                    last_name   string
                    address     string
                    email       string
                    phone       string
                    post_code   string
    ```  
    "users_info_target" is also prepared in our docker image with the information above.
    If you want to test your own data assets, it's necessary to put them into Hive in the docker container first.  

3.  Click "Models" at the top left corner to watch all the models here, now there has been two new models named "TotalCount_users_info_src" and "TotalCount_users_info_target" created automatically by the new data asset creation.  
    You can create a new accuracy model for the two new data assets registered just now.  
    Click "Create DQ Model" button at the top left corner, choose the top left block "Accuracy", follow the steps below.  
    1)  Choose Source: find "users_info_src" in the left tree, select some or all attributes in the right block, click "Next".  
    2)  Choose Target: find "users_info_target" in the left tree, select the matching attributes with previous ones in the right block, click "Next".  
    3)  Mapping Source and Target: choose the first row "user_id" as "PK" which means "Primary Key", and select "Source Fields" of each row, to match the same item in source table, e.g. user_id maps to user_id, first_name maps to first_name.   
        Finish all the mapping, click "Next".  
    4)  Fill out the required table freely, "Schedule Type" is the calculation period.  
        Submit and save, you can see your new DQ model created in the models list.  

4.  Now you've created two data assets and three DQ models, the models are calculated automatically at background in the docker container.  
    Wait for about 20 minutes, results would be published to web UI. Then you can see the dashboards of your new models in "My Dashboard" page.  
    View the accuracy model, there will be a "Deploy" button when the result comes out, click "Deploy" button to enable the periodically calculation of it, then you can get your dashboard growing by the period as you set.

### User data case guide

You can follow the steps [here](https://github.com/eBay/griffin/blob/master/griffin-doc/userDataCaseGuide.md) to use your own data for test.