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
import { Component, OnInit, OnChanges, SimpleChanges, OnDestroy,AfterViewInit,NgZone } from '@angular/core';
import {ChartService} from '../../service/chart.service';
import {ServiceService} from '../../service/service.service';

import { Router, ActivatedRoute, ParamMap } from '@angular/router';
import 'rxjs/add/operator/switchMap';
import {HttpClient} from '@angular/common/http';
import * as $ from 'jquery';

@Component({
  selector: 'app-detail-metric',
  templateUrl: './detail-metric.component.html',
  styleUrls: ['./detail-metric.component.css'],
  providers:[ChartService,ServiceService]
})
export class DetailMetricComponent implements OnInit {

  constructor(public chartService:ChartService,private route: ActivatedRoute,
  private router: Router,private http:HttpClient,private zone:NgZone,public servicecService:ServiceService
) {
    //     var self = this;
    // setTimeout(function () {
    //     self.currentMeasure = self.route.snapshot.paramMap.get('name');
    //     self.chartOption = self.chartService.getOptionBig(self.getData(self.currentMeasure));
    //     $('#bigChartDiv').height(window.innerHeight-120+'px');
    //     $('#bigChartDiv').width(window.innerWidth-400+'px');
    //     $('#bigChartContainer').show();
    // },200);
  };
  selectedMeasure:string;
  chartOption:{};
  data:any;
  currentMeasure:string;
  finalData:any;
//   metricData = {
// "hits" : {
//     "hits" : [
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493962623461,
//           "total" : 8043288,
//           "matched" : 8034775
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493973423461,
//           "total" : 9479698,
//           "matched" : 9476094
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493987823461,
//           "total" : 9194117,
//           "matched" : 9164237
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493995023461,
//           "total" : 9429018,
//           "matched" : 9375324
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493959023461,
//           "total" : 1086389,
//           "matched" : 1083336
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493973423461,
//           "total" : 1090650,
//           "matched" : 1090445
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1494009423461,
//           "total" : 8029660,
//           "matched" : 7979653
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493980623461,
//           "total" : 1088940,
//           "matched" : 1079003
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493995023461,
//           "total" : 1048833,
//           "matched" : 1047890
//         }
//       },
//       {
//         "_source" : {
//           "name" : "search_hourly",
//           "tmst" : 1493948223461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948224461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "search_hourly",
//           "tmst" : 1493948225461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948226461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948223461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948224461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948225461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948226461,
//           "total" : 100,
//           "matched" : 99
//         }
//       }
//     ]
//   }
// }
// ;

  ngOnInit() {
  	console.log('init');
  	this.currentMeasure = this.route.snapshot.paramMap.get('name');
    // this.finalData = this.getData(this.currentMeasure);
    var self = this;
      // let url_dashboard = this.servicecService.config.uri.dashboard;
    var metricDetailUrl = this.servicecService.config.uri.dashboard;
      // let data = this.metricData;
      this.http.post(metricDetailUrl, {"query": {  "bool":{"filter":[ {"term" : {"name": this.currentMeasure }}]}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).subscribe( data=> {
         var metric = {
           'name':'',
           'timestamp':0,
           'dq':0,
           'details':[]
         };
        this.data = data;
         metric.name = this.data.hits.hits[0]._source.name;
         metric.timestamp =this.data.hits.hits[this.data.hits.hits.length-1]._source.tmst;
         metric.dq = this.data.hits.hits[this.data.hits.hits.length-1]._source.matched/this.data.hits.hits[this.data.hits.hits.length-1]._source.matched*100;
         metric.details = new Array();
         for(let point of this.data.hits.hits){
             metric.details.push(point);
         };
         this.chartOption = this.chartService.getOptionBig(metric);
      $('#bigChartDiv').height(window.innerHeight-120+'px');
    $('#bigChartDiv').width(window.innerWidth-400+'px');
    $('#bigChartContainer').show();
         // return metric;
     });
    // setTimeout(function function_name(argument) {
    //   // body...
    
    // })
    
  }
  ngAfterContentInit (){
    console.log('after content init');
  }

  ngAfterContentChecked(){
    console.log('after content checked');
  }

  ngOnDestroy(){
  	console.log('destroy');
  }

  ngAfterViewInit(){
  	console.log('after view init')
  }

  ngAfterViewChecked(){
    console.log('after view checked');
  }

  onResize(event){
   this.resizeTreeMap();
  }

  resizeTreeMap(){
    $('#bigChartDiv').height( $('#mainWindow').height());
    $('#bigChartDiv').width( $('#mainWindow').width());
  }

  getData(metricName){
  	 
  }
}
