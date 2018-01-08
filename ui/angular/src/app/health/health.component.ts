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
// import {GetMetricService} from '../service/get-metric.service'
import {ServiceService} from '../service/service.service';

import * as $ from 'jquery';

@Component({
  selector: 'app-health',
  templateUrl: './health.component.html',
  styleUrls: ['./health.component.css'],
  // providers:[GetMetricService]
})
export class HealthComponent implements OnInit {

  constructor(private http: HttpClient,private router:Router,public serviceService:ServiceService) { };
  chartOption:object;
  // var formatUtil = echarts.format;
  orgs = [];
  dataData = [];
  finalData = [];
  oData = [];
  // originalData = [];
  originalData:any;
  metricName = [];
  metricNameUnique = [];
  myData = [];
  measureOptions = [];
  originalOrgs = [];
  orgWithMeasure: any;


  status:{
    'health':number,
    'invalid':number
  };
  // var formatUtil = echarts.format;
  metricData = [];

  
  onChartClick($event){
    if($event.data.name){
      this.router.navigate(['/detailed/'+$event.data.name]);
      window.location.reload();
    }
  }

  resizeTreeMap() {
    $('#chart1').height( $('#mainWindow').height() - $('.bs-component').outerHeight() );
  };

  parseData(data) {
    var sysId = 0;
    var metricId = 0;
    var result = [];
    for(let sys of data){
      var item = {
        'id':'',
        'name':'',
        children:[]
      };
      item.id = 'id_'+sysId;
      item.name = sys.name;
      if (sys.metrics != undefined) {
        item.children = [];
        for(let metric of sys.metrics){
          var itemChild = {
            id: 'id_' + sysId + '_' + metricId,
            name: metric.name,
            value: 1,
            dq: metric.dq,
            sysName: sys.name,
            itemStyle: {
              normal: {
                color: '#4c8c6f'
              }
            },
          };
          if (metric.dqfail == 1) {
            itemChild.itemStyle.normal.color = '#ae5732';
          } else {
            itemChild.itemStyle.normal.color = '#005732';
          }
          item.children.push(itemChild);
          metricId++;
        }
      }
      result.push(item);
      sysId ++;
    }
    return result;
   };

   getLevelOption() {
       return [
           {
               itemStyle: {
                   normal: {
                       borderWidth: 0,
                       gapWidth: 6,
                       borderColor: '#000'
                   }
               }
           },
           {
               itemStyle: {
                   normal: {
                       gapWidth: 1,
                       borderColor: '#fff'
                   }
               }
           }
       ];
   };

  renderTreeMap(res) {
    var data = this.parseData(res);
    var option = {
        title: {
            text: 'Data Quality Metrics Heatmap',
            left: 'center',
            textStyle:{
                color:'white'
            }
        },
        backgroundColor: 'transparent',
        tooltip: {
            formatter: function(info) {
                var dqFormat = info.data.dq>100?'':'%';
                if(info.data.dq)
                return [
                    '<span style="font-size:1.8em;">' + info.data.sysName + ' &gt; </span>',
                    '<span style="font-size:1.5em;">' + info.data.name+'</span><br>',
                    '<span style="font-size:1.5em;">dq : ' + info.data.dq.toFixed(2) + dqFormat + '</span>'
                ].join('');
            }
        },
        series: [
            {
                name:'System',
                type:'treemap',
                itemStyle: {
                    normal: {
                        borderColor: '#fff'
                    }
                },
                levels: this.getLevelOption(),
                breadcrumb: {
                    show: false
                },
                roam: false,
                nodeClick: 'link',
                data: data,
                width: '95%',
                bottom : 0
            }
        ]
    };
    this.resizeTreeMap();
    this.chartOption = option;
  };
  

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
        var array = [];
        node.metrics = array;
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
         self.renderTreeMap(self.oData);
      },1000) 
    });
  };

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

  ngOnInit() {
    var self = this;
    this.renderData();
       // this.renderTreeMap(this.getMetricService.renderData());
       // setTimeout(function function_name(argument) {
       //   // body...
       //     self.renderTreeMap(self.renderData());

       // })
  };
}
