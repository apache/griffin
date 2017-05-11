/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
define(['./module'], function (services) {
    'use strict';
    //version
    services.value('version', '0.1');

    services.factory('$config', function(){


    var BACKEND_SERVER = '';
//      var BACKEND_SERVER = 'http://localhost:8080';
    //   var BACKEND_SERVER = 'http://localhost:8080'; //dev env


//    var BACKEND_SERVER = 'http://10.249.74.51';


      var API_ROOT_PATH = '/api/v1';

      var config = {
          // URI paths, always have a trailing /
          uri: {
              base: BACKEND_SERVER + API_ROOT_PATH,

              dbtree:BACKEND_SERVER + '/metadata/hive/alltables',
              schemadefinition: BACKEND_SERVER + '/metadata/hive',
              dataassetlist: BACKEND_SERVER + '/metadata/hive/default/alltables',

              adddataasset: BACKEND_SERVER + API_ROOT_PATH + '/dataassets',
              updatedataasset: BACKEND_SERVER + API_ROOT_PATH + '/dataassets',
              getdataasset: BACKEND_SERVER + API_ROOT_PATH + '/dataassets',
              deletedataasset: BACKEND_SERVER + API_ROOT_PATH + '/dataassets',

              //mydashboard
              getmydashboard: BACKEND_SERVER + API_ROOT_PATH + '/metrics/mydashboard/',
              getsubscribe: BACKEND_SERVER + API_ROOT_PATH + '/subscribe/',
              newsubscribe: BACKEND_SERVER + API_ROOT_PATH + '/subscribe',

              //metrics
              statistics: BACKEND_SERVER + API_ROOT_PATH + '/metrics/statics',
              briefmetrics: BACKEND_SERVER + API_ROOT_PATH + '/metrics/briefmetrics',
              heatmap: BACKEND_SERVER + API_ROOT_PATH + '/metrics/heatmap' ,
              metricdetail: BACKEND_SERVER + API_ROOT_PATH + '/metrics/complete',
              rulemetric: BACKEND_SERVER + API_ROOT_PATH + '/metrics/brief',
//              dashboard: BACKEND_SERVER + API_ROOT_PATH + '/metrics/dashboard' ,
              organization:'http://10.249.75.250:8080/org',

              dashboard:'http://10.64.199.198:9200/griffin/accuracy/_search?pretty',
              metricsample: BACKEND_SERVER + API_ROOT_PATH + '/metrics/sample',
              metricdownload: BACKEND_SERVER + API_ROOT_PATH + '/metrics/download',

              //Models
              allModels: BACKEND_SERVER + '/measures',
              deleteModel:BACKEND_SERVER + '/measures/deleteByName',
              getModel: BACKEND_SERVER + '/measures/findByName',
              enableModel: BACKEND_SERVER + API_ROOT_PATH + '/models/enableModel',

              newAccuracyModel: BACKEND_SERVER + API_ROOT_PATH + '/models' ,
              newValidityModel: BACKEND_SERVER + API_ROOT_PATH + '/models' ,
              newAnomalyModel: BACKEND_SERVER + API_ROOT_PATH + '/models' ,
              newPublishModel: BACKEND_SERVER + API_ROOT_PATH + '/models' ,
              // newAccuracyModel: BACKEND_SERVER + API_ROOT_PATH + '/models/newAccuracyModel' ,
              // newValidityModel: BACKEND_SERVER + API_ROOT_PATH + '/model/newValidityModel' ,
              // newAnomalyModel: BACKEND_SERVER + API_ROOT_PATH + '/model/newAnomalyModel' ,
              // newPublishModel: BACKEND_SERVER + API_ROOT_PATH + '/model/newPublishModel' ,
              // getAccuracyModel: BACKEND_SERVER + API_ROOT_PATH + '/model/getAccuracyModel',
              // getValidityModel: BACKEND_SERVER + API_ROOT_PATH + '/model/getValidityModel',
              // getPublishModel: BACKEND_SERVER + API_ROOT_PATH + '/model/getPublishModel',
              // getAnomalyModel: BACKEND_SERVER + API_ROOT_PATH + '/model/getAnomalyModel',

              //Notification
              getnotifications: BACKEND_SERVER + API_ROOT_PATH + '/notifications',
          }

      };

      return config;
    });
});
