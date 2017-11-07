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
import  {DatePipe} from '@angular/common';
import {ServiceService} from '../service/service.service';
import {TruncatePipe} from './truncate.pipe';
// import {GetMetricService} from '../service/get-metric.service';
import * as $ from 'jquery';

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.css'],
  // pipes: [TruncatePipe],
  providers:[ChartService,ServiceService]
})
export class SidebarComponent implements OnInit {

  constructor(private http: HttpClient,
  	private router:Router,
    public serviceService:ServiceService,
  	public chartService:ChartService) {
  }
  
  oData = [];
  orgs = [];
  finalData = [];
  originalOrgs = [];
  status:{
  	'health':number,
  	'invalid':number
  };
  metricData = [];
  originalData :any;
  metricName = [];
  metricNameUnique = [];
  myData = [];
  chartOption = new Map();
  orgWithMeasure:any;
  measureOptions = [];
  // var formatUtil = echarts.format;

  pageInit() {
    // var allDataassets = this.serviceService.config.uri.dataassetlist;
    var health_url = this.serviceService.config.uri.statistics;
        this.http.get(health_url).subscribe(data => {
          // this.status.health = data.healthyJobCount;
          // this.status.invalid = data.jobCount - data.healthyJobCount;
          // renderDataAssetPie(this.status);
          this.sideBarList(null);
        },err => {});
  }
  
  onResize(event){
    // console.log('sidebar resize');
    if(window.innerWidth < 992) {
      $('#rightbar').css('display', 'none');
    } else {
      $('#rightbar').css('display', 'block');
      this.resizeSideChart();
    }
  }

  resizeSideChart(){
    $('#side-bar-metrics').css({
           height: $('#mainContent').height()-$('#side-bar-stats').outerHeight()+70
    });
    for(let i=0;i<this.oData.length;i++){
      for(let j=0;j<this.oData[i].metrics.length;j++){
        if (!this.oData[i].metrics[j].tag) {
          this.draw(this.oData[i].metrics[j], i, j);
        }
      }
    }
  }

   draw (metric, parentIndex, index) {
   		$('#'+this.oData[parentIndex].name+index).toggleClass('collapse');
       var chartId = 'chart' + parentIndex + '-' + index;
       document.getElementById(chartId).style.width = ($('.panel-heading').innerWidth()-40)+'px';
       document.getElementById(chartId).style.height = '200px';
       this.chartOption.set(chartId,this.chartService.getOptionSide(metric));
       var self = this;
       $('#'+chartId).unbind('click');
       $('#'+chartId).click(function(e) {
         self.router.navigate(['/detailed/'+self.oData[parentIndex].metrics[index].name]) ;
       });
   };

   getOption(parent,i){
   	return this.chartOption.get('chart'+parent+'-'+i);
   }

    sideBarList(sysName){
    	// this.finalData = this.getMetricService.renderData();
      var url_organization = this.serviceService.config.uri.organization;
      let url_dashboard = this.serviceService.config.uri.dashboard;
      this.http.get(url_organization).subscribe(data => {
      this.orgWithMeasure = data;
      var orgNode = null;
      for(let orgName in this.orgWithMeasure){
        orgNode = new Object();
        orgNode.name = orgName;
        orgNode.jobMap = [];
        orgNode.measureMap = [];
        var node = null;
        node = new Object();
        node.name = orgName;
        node.dq = 0;
        //node.metrics = new Array();
        var metricNode = {
          'name':'',
          'timestamp':'',
          'dq':0,
          'details':[]
        }
        var array = [];
        node.metrics = array;
        for(let key in this.orgWithMeasure[orgName]){
          orgNode.measureMap.push(key);
          this.measureOptions.push(key);
          var jobs = this.orgWithMeasure[orgName][key];          
            for(let i = 0;i < jobs.length;i++){
               orgNode.jobMap.push(jobs[i].jobName);
               var job = jobs[i].jobName;
               this.http.post(url_dashboard, {"query": {  "bool":{"filter":[ {"term" : {"name.keyword": job }}]}},  "sort": [{"tmst": {"order": "desc"}}],"size":300}).subscribe( data=> { 
                 this.originalData = data;
                 if(this.originalData.hits){
                   this.metricData = this.originalData.hits.hits;
                   metricNode.details = this.metricData;                                
                   metricNode.name = this.metricData[0]._source.name;
                   metricNode.timestamp = this.metricData[0]._source.tmst;
                   metricNode.dq = this.metricData[0]._source.value.matched/this.metricData[0]._source.value.total*100;
                   node.metrics.push(Object.assign({}, metricNode));
                 }

            },
               err => {
               console.log('Error occurs when connect to elasticsearh!');
               });           
            }                           
        } 
          this.finalData.push(node); 
          this.orgs.push(orgNode);                
      }
      this.oData = this.finalData.slice(0);
    });
    }

  ngOnInit() {
  	this.sideBarList(null);
  }

}
