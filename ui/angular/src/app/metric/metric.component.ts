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
import { Component, OnInit } from '@angular/core';
import  {HttpClient} from '@angular/common/http';
import  {Router} from "@angular/router";
import {ChartService} from '../service/chart.service';
// import {GetMetricService} from '../service/get-metric.service';
import {ServiceService} from '../service/service.service';
import * as $ from 'jquery';

@Component({
  selector: 'app-metric',
  templateUrl: './metric.component.html',
  styleUrls: ['./metric.component.css'],
  providers:[ChartService,ServiceService]
})
export class MetricComponent implements OnInit {

  constructor(
  	public chartService:ChartService,
  	// public getMetricService:GetMetricService,
    public serviceService:ServiceService,
  	private http: HttpClient,
  	private router:Router) { }
  orgs = [];
  // finalData :any;
  data :any;
  finalData = [];
  originalOrgs = [];
  status:{
  	'health':number,
  	'invalid':number
  };
  chartOption = new Map();
  // var formatUtil = echarts.format;
  dataData = [];
  // originalData = [];
  originalData:any;
  metricName = [];
  metricNameUnique = [];
  myData = [];
  measureOptions = [];
  selectedMeasureIndex = 0;
  chartHeight:any;
  selectedOrgIndex = 0;
  // var formatUtil = echarts.format;
  metricData = [];
  orgWithMeasure:any;
  
  // metricData = {
  // "hits" : {
  //   "hits" : [
  //     {
  //       "_source" : {
  //         "name" : "xixi",
  //         "tmst" : 1493962623461,
  //         "total" : 8043288,
  //         "matched" : 8034775
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "xixi",
  //         "tmst" : 1493973423461,
  //         "total" : 9479698,
  //         "matched" : 9476094
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "xixi",
  //         "tmst" : 1493987823461,
  //         "total" : 9194117,
  //         "matched" : 9164237
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "xixi",
  //         "tmst" : 1493995023461,
  //         "total" : 9429018,
  //         "matched" : 9375324
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "xixi",
  //         "tmst" : 1494009423461,
  //         "total" : 8029660,
  //         "matched" : 7979653
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "haha",
  //         "tmst" : 1493959023461,
  //         "total" : 1086389,
  //         "matched" : 1083336
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "haha",
  //         "tmst" : 1493973423461,
  //         "total" : 1090650,
  //         "matched" : 1090445
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "haha",
  //         "tmst" : 1493980623461,
  //         "total" : 1088940,
  //         "matched" : 1079003
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "haha",
  //         "tmst" : 1493995023461,
  //         "total" : 1048833,
  //         "matched" : 1047890
  //       }
  //     },
  //     {
  //       "_source" : {
  //         "name" : "haha",
  //         "tmst" : 1494013023461,
  //         "total" : 1063349,
  //         "matched" : 1055783
  //       }
  //     }
  //   ]
  // }
  // };
//   allData = {
//     "hits": {
//         "hits": [
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509519051606,
//                     "value": {
//                         "__tmst": 1509519051606,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509520857510,
//                     "value": {
//                         "__tmst": 1509520857510,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509521461108,
//                     "value": {
//                         "__tmst": 1509521461108,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509522049476,
//                     "value": {
//                         "__tmst": 1509522049476,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509525642052,
//                     "value": {
//                         "__tmst": 1509525642052,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measure-BA-0-1508478922000",
//                     "tmst": 1509534346669,
//                     "value": {
//                         "__tmst": 1509534346669,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measureName-BA-0-1509430761000",
//                     "tmst": 1509535555023,
//                     "value": {
//                         "__tmst": 1509535555023,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measureName-BA-0-1509430761000",
//                     "tmst": 1509537356548,
//                     "value": {
//                         "__tmst": 1509537356548,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 124989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measureName-BA-0-1509431586000",
//                     "tmst": 1509537947647,
//                     "value": {
//                         "__tmst": 1509537947647,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 104989
//                     }
//                 }
//             },
//             {
//                 "_source": {
//                     "name": "measureName-BA-0-1509431586000",
//                     "tmst": 1509540049924,
//                     "value": {
//                         "__tmst": 1509540049924,
//                         "miss": 11,
//                         "total": 125000,
//                         "matched": 122989
//                     }
//                 }
//             }
//         ]
//     }
// }
  public duplicateArray() {
  let arr = [];
  this.finalData.forEach((x) => {
    arr.push(Object.assign({}, x));
  });
  console.log(arr);
  // arr.map((x) => {x.status = DEFAULT});
  return this.finalData.concat(arr);
  }

  ngOnInit() {
    this.renderData();
  	// var self = this;
   //  // self.finalData = self.getMetricService.renderData();
   //  // self.finalData = self.renderData();
   //  // self.originalData = JSON.parse(JSON.stringify(self.finalData));
   //  self.data = self.renderData();
  	// setTimeout(function(){
  	// 	// body...
   //    // if(self.getMetricService.renderData()){
      
  	// 	// self.redraw(self.finalData);
   //    // self.redraw(self.renderData());
   //    self.redraw(self.data);
   //    // }
  	// },0);
  	
  }
  

  renderData(){
    var url_organization = this.serviceService.config.uri.organization;
    // this.http.get(url_organization).subscribe(data => {
      this.orgWithMeasure = {
    "orgName1": {
        "measureName2": [
            "measureName-BA-0-1509431586000",
            "measure-BA-0-1508478922000"
        ],
        "measureName": [
            "measureName-BA-0-1509431586000",
            "measureName-BA-0-2509430761000"
        ]
    },
    "orgName2": {
        "measureName3": [
            "measure-BA-0-1508478922000"
        ],
        "measureName4": [
            "measureName-BA-0-1509431586000",
            "measureName-BA-0-1509430761000"
        ]
    }
};
      // let orgWithMeasure = data;
      var orgNode = null;
      for(let orgName in this.orgWithMeasure){
        orgNode = new Object();
        orgNode.name = orgName;
        orgNode.jobMap = [];
        orgNode.measureMap = [];
        for(let key in this.orgWithMeasure[orgName]){
          orgNode.measureMap.push(key);
          this.measureOptions.push(key);
          for(let i = 0;i < this.orgWithMeasure[orgName][key].length;i++){
            orgNode.jobMap.push(this.orgWithMeasure[orgName][key][i]);
          }
        }
        this.orgs.push(orgNode);
      }
      this.originalOrgs = this.orgs;
      // console.log(this.originalOrgs);
      let url_dashboard = this.serviceService.config.uri.dashboard;
      this.http.post(url_dashboard, {"query": {"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).subscribe(data => {
            this.originalData = data;
            this.myData = JSON.parse(JSON.stringify(this.originalData.hits.hits));
            // this.myData = this.allData.hits.hits;
            this.metricName = [];
            // for(var i = 0;i<this.myData.length;i++){
            //     this.metricName.push(this.myData[i]._source.name);
            // }
            for(var i = 0;i<this.myData.length;i++){
                this.metricName.push(this.myData[i]._source.name);
            }
            this.metricNameUnique = [];
            for(let name of this.metricName){
                if(this.metricNameUnique.indexOf(name) === -1){
                    this.metricData[this.metricNameUnique.length] = new Array();
                    this.metricNameUnique.push(name);
                }
            };
            for(var i = 0;i<this.myData.length;i++){
                for(var j = 0 ;j<this.metricNameUnique.length;j++){
                    if(this.myData[i]._source.name==this.metricNameUnique[j]){
                        this.metricData[j].push(this.myData[i]);
                    }
                }
            }
            for(let sys of this.originalOrgs){
                var node = null;
                node = new Object();
                node.name = sys.name;
                node.dq = 0;
                node.metrics = new Array();
                for (let metric of this.metricData){
                    if(sys.jobMap.indexOf(metric[metric.length-1]._source.name)!= -1){
                        var metricNode = {
                            'name':'',
                            'timestamp':'',
                            'dq':0,
                            'details':[]
                        }
                        metricNode.name = metric[metric.length-1]._source.name;
                        metricNode.timestamp = metric[metric.length-1]._source.tmst;
                        metricNode.dq = metric[metric.length-1]._source.value.matched/metric[metric.length-1]._source.value.total*100;
                        metricNode.details = metric;
                        node.metrics.push(metricNode);
                    }
                }
                this.finalData.push(node);
            }
            this.originalData = JSON.parse(JSON.stringify(this.finalData));
            var self = this;
            setTimeout(function function_name(argument) {
              // body...
              self.redraw(self.finalData);

            },0)
            console.log(this.finalData);
            // return JSON.parse(JSON.stringify(this.finalData));
            return this.finalData;
      });
    // });
  };


  getOption(parent,i){
   	return this.chartOption.get('thumbnail'+parent+'-'+i);
   }

	// this.originalData = angular.copy(this.finalData);
	    // if($routeParams.sysName && this.originalData && this.originalData.length > 0){
	    //   for(var i = 0; i < this.originalData.length; i ++){
	    //     if(this.originalData[i].name == $routeParams.sysName){
	    //       this.selectedOrgIndex = i;
	    //       this.changeOrg();
	    //       this.orgSelectDisabled = true;
	    //       break;
	    //     }
	    //   }
	    // }
	    // $timeout(function() {
	    //     redraw(this.finalData);
	    // });
	   // });
	// });
//          $http.post(url_dashboard, {"query": {"match_all":{}},"size":1000}).success(function(res) {

  redraw (data) {
    this.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';
      for(let i = 0;i<data.length;i++){
          var parentIndex = i;
          for(let j = 0;j<data[i].metrics.length;j++){
          	let index = j;
          	let chartId = 'thumbnail' + parentIndex + '-' + index;
            $('#thumbnail'+parentIndex+'-'+index).get(0).style.width = $('#thumbnail'+parentIndex+'-'+index).parent().width()+'px';
            $('#thumbnail'+parentIndex+'-'+index).get(0).style.height = this.chartHeight;
  			    this.chartOption.set(chartId,this.chartService.getOptionThum(data[i].metrics[j]));
          }
      }
  }

  goTo(parent,i){
   	this.router.navigate(['/detailed/'+this.finalData[parent].metrics[i].name]) ;
  }

  changeOrg() {
      this.selectedMeasureIndex = undefined;
      this.measureOptions = [];
      this.finalData = [];
      if(this.selectedOrgIndex == 0){
        for(let data of this.originalData){
      		this.finalData.push(data);
        }
      }
      else {
        var org = this.originalData[this.selectedOrgIndex-1];
        for(let key in this.orgWithMeasure){
           if(key == org.name){
              for(let measure in this.orgWithMeasure[key]){
                this.measureOptions.push(measure);
              }
           }
        }
        this.finalData.push(org);
        // console.log(this.finalData);
        // for(let metric of org.metrics){
        // 	if(this.measureOptions.indexOf(metric.name) == -1){
        // 		this.measureOptions.push(metric.name);
        // 	}
        // }

      }
      var self = this;
      // self.data = self.renderData();
      setTimeout(function() {
          // self.redraw(self.finalData);
          self.redraw(self.finalData);
      }, 0);
      // console.log(this.originalData);
  };

  changeMeasure() {
      this.finalData = [];
      var jobdetail = [];
      if(this.selectedOrgIndex == 0){
      	for(let data of this.originalData){
      		this.finalData.push(data);
      	}
      } else {
        var org = JSON.parse(JSON.stringify(this.originalData[this.selectedOrgIndex-1]));   
        this.finalData.push(org);
      // }
      if(this.selectedMeasureIndex != undefined && this.selectedMeasureIndex != 0){
        var measure = this.measureOptions[this.selectedMeasureIndex-1];
        for(let key in this.orgWithMeasure){
           if(key == org.name){
              for(let measurename in this.orgWithMeasure[key]){
                if(measurename == measure){
                  // console.log(this.orgWithMeasure[key][measurename]);
                  for(let i=0;i< this.orgWithMeasure[key][measurename].length;i++){
                    jobdetail.push(this.orgWithMeasure[key][measurename][i]);
                     


                  }
                }
              }
           }
        }
        	for(let sys of this.finalData){
            let oldMetrics = sys.metrics;
            sys.metrics = [];
            for(let i = 0;i<oldMetrics.length;i++){
              
              // console.log(jobdetail);
              for(let j=0;j< jobdetail.length;j++){
                console.log(oldMetrics[i].name);
                console.log(jobdetail[j]);
                if(oldMetrics[i].name == jobdetail[j]) {
                sys.metrics.push(oldMetrics[i]);
              }
              }
              
            };
        };
      }
      }
      var self = this;
      // self.data = self.renderData();
      setTimeout(function() {
          // self.redraw(self.finalData);
          self.redraw(self.finalData);
      }, 0);
      // console.log(this.originalData);
  }

        // function resizePieChart() {
        //   $('#data-asset-pie').css({
        //       height: $('#data-asset-pie').parent().width(),
        //       width: $('#data-asset-pie').parent().width()
        //   });
        // }

        // this.$on('resizeHandler', function() {
        //   if($route.current.$$route.controller == 'MetricsCtrl') {
        //     console.log('metrics resize');
        //     redraw(this.dataData);
        //   }
        // });
}
