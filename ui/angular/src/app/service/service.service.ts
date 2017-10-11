/*
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
*/
import { Injectable } from '@angular/core';

@Injectable()
export class ServiceService {

  constructor() { }
  	// public this.BACKEND_SERVER = 'http://10.149.247.156:38080';
    //public BACKEND_SERVER = 'http://localhost:8080';
    public BACKEND_SERVER = '';
    public API_ROOT_PATH = '/api/v1';
    public ES_SERVER = "http://" + location.host.replace("8080", "9200");
    //public ES_SERVER = "http://10.64.222.80:39200" ;

    public config = {
          // URI paths, always have a trailing /
          uri: {
              base: this.BACKEND_SERVER + this.API_ROOT_PATH,

              dbtree:this.BACKEND_SERVER + '/metadata/hive/allTables',
              schemadefinition: this.BACKEND_SERVER + '/metadata/hive/table',
              dataassetlist: this.BACKEND_SERVER + '/metadata/hive/allTables',


              // adddataasset: this.BACKEND_SERVER + this.API_ROOT_PATH + '/dataassets',
              // updatedataasset: this.BACKEND_SERVER + this.API_ROOT_PATH + '/dataassets',
              getdataasset: this.BACKEND_SERVER + this.API_ROOT_PATH + '/dataassets',
              // deletedataasset: this.BACKEND_SERVER + this.API_ROOT_PATH + '/dataassets',

              //mydashboard
              getmydashboard: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/mydashboard/',
              // getsubscribe: this.BACKEND_SERVER + this.API_ROOT_PATH + '/subscribe/',
              // newsubscribe: this.BACKEND_SERVER + this.API_ROOT_PATH + '/subscribe',

              //metrics
              statistics: this.BACKEND_SERVER + '/jobs/health',
              // briefmetrics: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/briefmetrics',
              heatmap: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/heatmap' ,
              // metricdetail: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/complete',
              // rulemetric: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/brief',
//              organization:this.BACKEND_SERVER+'/org',
              orgmap: this.BACKEND_SERVER+'/metrics/org',


              metricsByOrg:'',
//              organization:'/org.json',
//              dashboard:'/dashboard.json',

              organization:this.BACKEND_SERVER + '/orgWithMetricsName',
              dashboard:this.ES_SERVER+'/griffin/accuracy/_search?pretty&filter_path=hits.hits._source',
              metricsample: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/sample',
              metricdownload: this.BACKEND_SERVER + this.API_ROOT_PATH + '/metrics/download',

              //Models
              allModels: this.BACKEND_SERVER + '/measures',
              addModels: this.BACKEND_SERVER + '/measure',
              deleteModel:this.BACKEND_SERVER + '/measure',
              getModel: this.BACKEND_SERVER + '/measure',
              enableModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models/enableModel',

              //Jobs
              allJobs:this.BACKEND_SERVER + '/jobs/',
              addJobs:this.BACKEND_SERVER+'/jobs',
              getMeasuresByOwner:this.BACKEND_SERVER+'/measures/owner/',
              deleteJob:this.BACKEND_SERVER + '/jobs',
              getInstances:this.BACKEND_SERVER + '/jobs/instances',
//              allJobs:'/jobs.json',
              newAccuracyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models' ,
              newValidityModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models' ,
              newAnomalyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models' ,
              newPublishModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models' ,
              // newAccuracyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/models/newAccuracyModel' ,
              // newValidityModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/newValidityModel' ,
              // newAnomalyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/newAnomalyModel' ,
              // newPublishModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/newPublishModel' ,
              // getAccuracyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/getAccuracyModel',
              // getValidityModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/getValidityModel',
              // getPublishModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/getPublishModel',
              // getAnomalyModel: this.BACKEND_SERVER + this.API_ROOT_PATH + '/model/getAnomalyModel',

              //Notification
              getnotifications: this.BACKEND_SERVER + this.API_ROOT_PATH + '/notifications'
          }

      };
}
