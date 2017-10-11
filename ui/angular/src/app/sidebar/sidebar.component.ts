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
// import {GetMetricService} from '../service/get-metric.service';
import * as $ from 'jquery';

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.css'],
  providers:[ChartService,ServiceService]
})
export class SidebarComponent implements OnInit {

  constructor(private http: HttpClient,
  	private router:Router,
    public servicecService:ServiceService,
  	public chartService:ChartService) {
  }

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
  // var formatUtil = echarts.format;

  pageInit() {
    // var allDataassets = this.servicecService.config.uri.dataassetlist;
    var health_url = this.servicecService.config.uri.statistics;
        this.http.get(health_url).subscribe(data => {
          // this.status.health = data.healthyJobCount;
          // this.status.invalid = data.jobCount - data.healthyJobCount;
          // renderDataAssetPie(this.status);
          this.sideBarList(null);
        },err => {});
  }
  
  onResize(event){
    console.log('sidebar resize');
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
    for(let i=0;i<this.finalData.length;i++){
      for(let j=0;j<this.finalData[i].metrics.length;j++){
        if (!this.finalData[i].metrics[j].tag) {
          this.draw(this.finalData[i].metrics[j], i, j);
        }
      }
    }
  }

   draw (metric, parentIndex, index) {
   		$('#'+this.finalData[parentIndex].name+index).toggleClass('collapse');
       var chartId = 'chart' + parentIndex + '-' + index;
       document.getElementById(chartId).style.width = ($('.panel-heading').innerWidth()-40)+'px';
       document.getElementById(chartId).style.height = '200px';
       this.chartOption.set(chartId,this.chartService.getOptionSide(metric));
       var self = this;
       $('#'+chartId).unbind('click');
       $('#'+chartId).click(function(e) {
         self.router.navigate(['/detailed/'+self.finalData[parentIndex].metrics[index].name]) ;
       });
   };

   getOption(parent,i){
   	return this.chartOption.get('chart'+parent+'-'+i);
   }

    sideBarList(sysName){
    	// this.finalData = this.getMetricService.renderData();
      var url_organization = this.servicecService.config.uri.organization;
    this.http.get(url_organization).subscribe(data => {
      let orgWithMeasure = data;
      var orgNode = null;
      for(let orgName in orgWithMeasure){
        orgNode = new Object();
        orgNode.name = orgName;
        orgNode.measureMap = orgWithMeasure[orgName];
        this.orgs.push(orgNode);
      }
      this.originalOrgs = this.orgs;
      let url_dashboard = this.servicecService.config.uri.dashboard;
      this.http.post(url_dashboard, {"query": {"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).subscribe(data => {
            // this.originalData = JSON.parse(JSON.stringify(data));
            this.originalData = data;
            this.myData = JSON.parse(JSON.stringify(this.originalData.hits.hits));
            this.metricName = [];
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
                    if(sys.measureMap.indexOf(metric[metric.length-1]._source.name)!= -1){
                        var metricNode = {
                            'name':'',
                            'timestamp':'',
                            'dq':0,
                            'details':[]
                        }
                        metricNode.name = metric[metric.length-1]._source.name;
                        metricNode.timestamp = metric[metric.length-1]._source.tmst;
                        metricNode.dq = metric[metric.length-1]._source.matched/metric[metric.length-1]._source.total*100;
                        metricNode.details = metric;
                        node.metrics.push(metricNode);
                    }
                }
                this.finalData.push(node);
            }
            console.log(this.finalData);
            // return JSON.parse(JSON.stringify(this.finalData));
            return this.finalData;
      });
    });
    }

  ngOnInit() {
  	this.sideBarList(null);
  }

}
