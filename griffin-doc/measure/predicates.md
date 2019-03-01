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

#About predicates

##Overview
Sometimes we need to check certain conditions before starting a measurement. Depending on these conditions, start or not start the measurement. For example, check whether the file. For these purposes, serve as predicate functional.

##Configure predicates

For configuring predicates need add property to measure json:
```
{
    ...
     "data.sources": [
        ...
         "connectors": [
                   "predicates": [
                       {
                         "type": "file.exist",
                         "config": {
                           "root.path": "/path/to/",
                           "path": "file.ext,file2.txt"
                         }
                       }
                   ],
         ...
         
     ]
}
```

Possible values for predicates.type:
- "file.exist" - in this case creates predicate with class org.apache.griffin.core.job.FileExistPredicator. This predicate checks existence of files before starting Spark jobs.
 ```
                     {
                         "type": "file.exist",
                         "config": {
                           "root.path": "/path/to/",
                           "path": "file.ext,file2.txt"
                         }
                     }
```

- "custom" - in this case required transmit class name in the property "class" in config. 
This example creates same predicate like in previous example
```
                     {
                         "type": "custom",
                         "config": {
                           "class": "org.apache.griffin.core.job.FileExistPredicator",
                           "root.path": "/path/to/",
                           "path": "file.ext,file2.txt"
                         }
                     }
```
It important to notice that predicate class must satisfy follow conditions:
- implement interface **org.apache.griffin.core.job.Predicator**
- have constructor with argument of type **org.apache.griffin.core.job.entity.SegmentPredicate**


