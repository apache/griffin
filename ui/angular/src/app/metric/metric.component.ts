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
  oData = [];
  mData = [];
  fData = [];
  originalOrgs = [];
  status:{
  	'health':number,
  	'invalid':number
  };
  chartOption = new Map();
  dataData = [];
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
  alljobs = [];
  

  public duplicateArray() {
  let arr = [];
  this.oData.forEach((x) => {
    arr.push(Object.assign({}, x));
  });
  // arr.map((x) => {x.status = DEFAULT});
  return this.oData.concat(arr);
  }

  ngOnInit() {
    this.renderData();	
  }
  
  renderData(){
    var url_organization = this.serviceService.config.uri.organization;
    let url_dashboard = this.serviceService.config.uri.dashboard;
    this.http.get(url_organization).subscribe(data => {
      var jobMap = new Map();
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
        node.metrics = [];
        for(let key in this.orgWithMeasure[orgName]){
          orgNode.measureMap.push(key);
          this.measureOptions.push(key);
          var jobs = this.orgWithMeasure[orgName][key];          
            for(let i = 0;i < jobs.length;i++){
              orgNode.jobMap.push(jobs[i].jobName);
              var job = jobs[i].jobName;
              jobMap.set(job, orgNode.name);
              this.http.post(url_dashboard, {"query": {  "bool":{"filter":[ {"term" : {"name.keyword": job }}]}},  "sort": [{"tmst": {"order": "desc"}}],"size":300}).subscribe( jobes=> { 
                this.originalData = jobes;
                if(this.originalData.hits){
                  this.metricData = this.originalData.hits.hits;
                  if(this.metricData[0]._source.value.miss != undefined){
                    metricNode.details = this.metricData;                                
                    metricNode.name = this.metricData[0]._source.name;
                    metricNode.timestamp = this.metricData[0]._source.tmst;
                    metricNode.dq = this.metricData[0]._source.value.matched/this.metricData[0]._source.value.total*100;
                    this.pushToNode(jobMap, metricNode);
                  }
                }
              },
              err => {
                // console.log(err);
              console.log('Error occurs when connect to elasticsearh!');
              });            
            }                          
        } 
        this.finalData.push(node); 
        this.orgs.push(orgNode);                 
      }
      this.oData = this.finalData.slice(0);
      var self = this;
      setTimeout(function function_name(argument) {
        self.redraw(self.oData);
      },1000) 
    });
  }

  pushToNode(jobMap, metricNode){
    var jobName = metricNode.name;
    var orgName = jobMap.get(jobName);
    var org = null;
    for(var i = 0; i < this.finalData.length; i ++){
      org = this.finalData[i];
      if(orgName == org.name){
        org.metrics.push(Object.assign({}, metricNode));
      }
    }
  }  


  getOption(parent,i){
   	return this.chartOption.get('thumbnail'+parent+'-'+i);
  }

  redraw (data) {
    this.chartHeight = $('.chartItem:eq(0)').width()*0.8+'px';
    for(let i = 0;i<data.length;i++){
      var parentIndex = i;
      for(let j = 0;j<data[i].metrics.length;j++){
      	let index = j;
      	let chartId = 'thumbnail' + parentIndex + '-' + index;
        let _chartId = '#' + chartId;
        var divs = $(_chartId);
        divs.get(0).style.width = divs.parent().width()+'px';
        divs.get(0).style.height = this.chartHeight;
  	  this.chartOption.set(chartId,this.chartService.getOptionThum(data[i].metrics[j]));
      }
    }
  }

  goTo(parent,i){
   	this.router.navigate(['/detailed/'+this.oData[parent].metrics[i].name]) ;
  }

  changeOrg() {
    this.selectedMeasureIndex = undefined;
    this.measureOptions = [];
    this.oData = this.finalData.slice(0);
    if(this.selectedOrgIndex == 0){
      this.oData = this.finalData;
    }
    else {
      var org = this.orgs[this.selectedOrgIndex-1];
      this.measureOptions = org.measureMap;
      for(let i = 0;i<this.oData.length;i++){
        if(this.oData[i].name!=org.name){
          for(var j = i; j < this.oData.length - 1; j++){
            this.oData[j] = this.oData[j + 1];
          }
          this.oData.length--;
          i--;
        }
      }
    }
    this.mData = this.oData.slice(0);
    var self = this;
    setTimeout(function() {
      self.redraw(self.oData);
    }, 1000);
  };

  changeMeasure() {
    var jobdetail = [];  
    this.fData = JSON.parse(JSON.stringify(this.mData));
    this.oData = this.fData; 
    if(this.selectedMeasureIndex != undefined && this.selectedMeasureIndex != 0){
      var measure = this.measureOptions[this.selectedMeasureIndex-1];
      for(let key in this.orgWithMeasure){
        if(key == this.fData[0].name){
          for(let measurename in this.orgWithMeasure[key]){
            if(measurename == measure){
              var jobname = this.orgWithMeasure[key][measurename];
              for(let i=0;i< jobname.length;i++){
                  jobdetail.push(jobname[i].jobName);
                }
            }
          }
        }
      }
      for(let i = 0;i<this.fData[0].metrics.length;i++){
        if(jobdetail.indexOf(this.fData[0].metrics[i].name) === -1){
          for(var j = i; j < this.fData[0].metrics.length - 1; j++){
            this.fData[0].metrics[j] = this.fData[0].metrics[j + 1];
          }
          this.fData[0].metrics.length--;
          i--;
        }          
      }
    }
    var self = this;
    setTimeout(function() {
      self.redraw(self.oData);
    }, 0);
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
