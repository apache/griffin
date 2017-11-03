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
        // this.router.navigate(['/detailed/'+$event.data.name]);
        this.router.navigate(['/detailed/'+$event.data.name]);
        window.location.reload();
        // window.location.href = '/detailed/'+$event.data.name;
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
    this.http.get(url_organization).subscribe(data => {
      this.orgWithMeasure = data;
      var orgNode = null;
      for(let orgName in this.orgWithMeasure){
        orgNode = new Object();
        orgNode.name = orgName;
        orgNode.jobMap = [];
        orgNode.measureMap = [];
        for(let key in this.orgWithMeasure[orgName]){
          orgNode.measureMap.push(key);
          this.measureOptions.push(key);
          // console.log(this.measureOptions);
          if(this.orgWithMeasure[orgName][key]!=null){
            for(let i = 0;i < this.orgWithMeasure[orgName][key].length;i++){
            orgNode.jobMap.push(this.orgWithMeasure[orgName][key][i].jobName);
          }
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
              self.renderTreeMap(self.finalData);

            },0)
            console.log(this.finalData);
            // return JSON.parse(JSON.stringify(this.finalData));
            return this.finalData;
      });
    });
  };

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
