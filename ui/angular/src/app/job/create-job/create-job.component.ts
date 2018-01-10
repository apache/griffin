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
import { Component, OnInit, AfterViewChecked, ViewChildren } from '@angular/core';
import { FormControl } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { MaxLengthValidator } from '@angular/forms';
import { NgControlStatus ,Validators } from '@angular/forms';
import { PatternValidator } from '@angular/forms';
// import {MdDatepickerModule} from '@angular/material';
import { MatDatepickerModule } from '@angular/material';
import { ServiceService } from '../../service/service.service';
import { AngularMultiSelectModule } from 'angular2-multiselect-dropdown/angular2-multiselect-dropdown';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService, ToasterConfig } from 'angular2-toaster';
import * as $ from 'jquery';
import  { HttpClient,HttpParams } from '@angular/common/http';
import  { Router } from "@angular/router";
import { NouisliderModule } from 'ng2-nouislider';

@Component({
  selector: 'app-create-job',
  templateUrl: './create-job.component.html',
  providers:[ServiceService],
  styleUrls: ['./create-job.component.css']
})
export class CreateJobComponent implements OnInit, AfterViewChecked {

  constructor(toasterService: ToasterService,private http: HttpClient,private router:Router,public serviceService:ServiceService) {
    this.toasterService = toasterService;
  };
  
  @ViewChildren('sliderRef') sliderRefs;
  // someRange=[-20, 0];
  someKeyboard = [];
  someKeyboardConfig = [];
  config:any;
  baseline :string;
  cronExp :string;
  dropdownList = [];
  currentStep = 1;
  Times = ['seconds','minutes','hours'];
  maskOpen = false;
  
  keyupLabelOn = false;
  keydownLabelOn = false;
  periodTime :number;
  StartTime = '';
  sourcePat :'';
  targetPat :'';
  createResult = '';
  jobStartTime : any;

  Measures:object;
  measureinfo:object;

  measure:string;
  measureid:any;
  ntAccount = 0;
  // newJob={
  //   "sourcePattern":'',
  //   "targetPattern":'',
  //   "jobStartTime":0,
  //   "interval":'',
  //   "groupName":'',
  // }

  newJob={
    "cron.expression": "",
    // "cron.time.zone": "GMT+8:00",
    // "predicate.config": {
    //   "interval": "1m",
    //   "repeat": 2
    // },
    "data.segments": [
      // {
      //   "data.connector.index": "source[0]",
      //   "segment.range": {
      //     "begin": "",
      //     "length": ""
      //   }
      // },
      // {
      //   "data.connector.index": "target[0]",
      //   "segment.range": {
      //     "begin": "",
      //     "length": ""
      //   }
      // }
    ]
  }

  beginTime = [];
  timeLength = [];
  originBegin = [];
  originLength = [];

  private toasterService: ToasterService;


  public visible = false;
  public visibleAnimate = false;

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => this.visible = false, 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains('modal')) {
      this.hide();
    }
  }
  
  showTime(evt){
    evt.target.nextElementSibling.style.display='';
  }

  OnMouseleave(evt){
    evt.target.style.display = 'none';
  }

  close(){
    this.maskOpen = false;
  }

  prev(){
  	history.back();
  }

  // submit (jobForm) {
  //     this.measureid = this.getMeasureId();
  //     // jobForm.markAsPristine();
  //     var period;
  //     if(this.timeType=='minutes')
  //         period = this.periodTime *60;
  //     else if(this.timeType=='hours')
  //         period = this.periodTime * 3600;
  //     else period = this.periodTime;
  //     var rule = '';
  //     var time :number;
  //     if(this.jobStartTime){
  //       var year = this.jobStartTime.getFullYear();
  //       var month = this.jobStartTime.getMonth() + 1;
  //       var day = this.jobStartTime.getDate();
  //       var startTime = year +'-'+ month + '-'+ day + ' '+ this.timeDetail;
  //     }

  //     time = Date.parse(startTime);
  //     if(isNaN(time)){
  //        this.toasterService.pop('error','Error!','Please input the right format of start time');
  //         return false;
  //     }
  //     if (!jobForm.valid) {
  //       this.toasterService.pop('error', 'Error!', 'Please complete the form!');
  //       return false;
  //     }
      
  //     this.newJob={
  //       "sourcePattern":this.sourcePat,
  //       "targetPattern":this.targetPat,
  //       "jobStartTime":time,
  //       "interval":period,
  //       "groupName":'BA',
  //     },
  //     this.visible = true;
  //     setTimeout(() => this.visibleAnimate = true, 100);
  // }
  // save() {
  // 	var date = new Date();
  // 	var datastr = date.toString();
  //   var month = date.getMonth()+1;
  //   var timestamp = Date.parse(datastr);
  //   var jobName = this.measure + '-BA-' + this.ntAccount + '-' + timestamp;
  //   var addJobs = this.serviceService.config.uri.addJobs;
  //   var newJob = addJobs + '?group=' + this.newJob.groupName + '&jobName=' + jobName + '&measureId=' + this.measureid;
  //   this.http
  //   .post(newJob, this.newJob)
  //   .subscribe(data => {
  //     this.createResult = data['results'];
  //     this.hide();
  //     this.router.navigate(['/jobs']);
  //   },
  //   err => {
  //     console.log('Error when creating job');
  //   });
  // }

  submit (jobForm){
    this.measureid = this.getMeasureId();
    this.newJob = {
      "cron.expression": this.cronExp,
      // "cron.time.zone": "GMT+8:00", 
      // "predicate.config": {
      // "interval": "1m",
      // "repeat": 2
      // },
      "data.segments": [
      // {
      //   "data.connector.index": "source[0]",
      //   "segment.range": {
      //   "begin": "",
      //   "length": ""
      //   }
      // },
      // {
      //   "data.connector.index": "target[0]",
      //   "segment.range": {
      //   "begin": "",
      //   "length": ""
      //   }
      // }
      ]
    }
    for(let i = 0;i < this.dropdownList.length;i++){
      var length = this.someKeyboard[i][1]-this.someKeyboard[i][0];
      this.newJob['data.segments'].push({
        "data.connector.name": this.dropdownList[i].connectorname,
        "as.baseline":true,
        "segment.range": {
          "begin": this.someKeyboard[i][0],
          "length": length
        }
      });
      this.originBegin.push(this.someKeyboard[i][0]);
      this.originLength.push(length);
    };
    if(this.dropdownList.length == 2){
      delete this.newJob['data.segments'][1]['as.baseline'];
    }
    console.log(this.newJob);
    this.visible = true;
    setTimeout(() => this.visibleAnimate = true, 100);
  }

  save() {
    var date = new Date();
    var datastr = date.toString();
    // var month = date.getMonth()+1;
    var timestamp = Date.parse(datastr);
    var jobName = this.measure + '-BA-' + this.ntAccount + '-' + timestamp;
    var addJobs = this.serviceService.config.uri.addJobs;
    var newJob = addJobs + '?group=' + '-BA-' + '&jobName=' + jobName + '&measureId=' + this.measureid;
    this.http
    .post(newJob, this.newJob)
    .subscribe(data => {
      this.createResult = data['results'];
      this.hide();
      this.router.navigate(['/jobs']);
    },
    err => {
      console.log('Error when creating job');
    });
  }

  onResize(event){
   this.resizeWindow();
  }

  resizeWindow(){
    var stepSelection = '.formStep';
    $(stepSelection).css({
      height: window.innerHeight - $(stepSelection).offset().top - $('#footerwrap').outerHeight()
    });
    $('fieldset').height($(stepSelection).height() - $(stepSelection + '>.stepDesc').height() - $('.btn-container').height() - 200);
    $('.y-scrollable').css({
      'height': $('fieldset').height()
    });
    $('#data-asset-pie').css({
      height: $('#data-asset-pie').parent().width(),
      width: $('#data-asset-pie').parent().width()
    });
  }

  setHeight(){
  	$('#md-datepicker-0').height(250);
  }

  getMeasureId(){
    for(let index in this.Measures){
      if(this.measure == this.Measures[index].name){
        return this.Measures[index].id;
      }     
    }
  }
  
  onChange(measure){
    this.dropdownList = [];
    for(let index in this.Measures){
      var map = this.Measures[index];
      if(measure == map.name){
        var source = map["data.sources"];
        // this.baseline = (source.length == 2) ? 'target[0]' : 'source[0]';
        for(let i = 0;i < source.length;i++){
          var details = source[i].connectors;
          for(let j = 0;j < details.length;j++){
            if(details[j]['data.unit']!=undefined){
              var table = details[j].config.database+'.'+details[j].config['table.name'];
              var size = details[j]['data.unit'];
              var connectorname = details[j]['name'];
              var detail = {"id":i+1,"name":table,"size":size,"connectorname":connectorname};
              this.dropdownList.push(detail);
            }
          }
        }
      }     
    }
    for(let i = 0;i < this.dropdownList.length;i++){
      this.someKeyboard[i] = [-1,0];
      this.someKeyboardConfig[i] = JSON.parse(JSON.stringify(this.config));
      if(this.sliderRefs._results[i]){
        this.sliderRefs._results[i].slider.updateOptions({
          range: {
            'min': -10,
            'max': 0
          }
        });
      }   
    }       
  }
  

  changeRange(index,value,i){
    let newRange = [];
    newRange[i] = [this.someKeyboard[i][0], this.someKeyboard[i][1]];
    newRange[i][index] = value;
    this.updateSliderRange(value,i);
    this.someKeyboard[i] = newRange[i];
  }
  
  rangeChange(evt,i){
    var oldmin = this.sliderRefs._results[i].config.range.min;
    if((evt[0] - oldmin)<=2){
      this.sliderRefs._results[i].slider.updateOptions({
        range: {
          'min': oldmin-10,
          'max': 0
        }
      }); 
    }
    if((evt[0] - oldmin)>=13){
      this.sliderRefs._results[i].slider.updateOptions({
        range: {
          'min': oldmin+10,
          'max': 0
        }
      }); 
    }
    this.someKeyboard[i] = evt; 
  }

  updateSliderRange(value,i){
    // setTimeout(() => { 
    var oldmin = this.sliderRefs._results[i].config.range.min;
    var oldmax = this.sliderRefs._results[i].config.range.max
    var newmin = Math.floor(value/10);
    if((value - oldmin)<=3){
      this.sliderRefs._results[i].slider.updateOptions({
        range: {
          'min': newmin*10,
          'max': 0
        }
      });
    }
    // }, 100)
  }

  blinkKeyupLabel() {
    this.keyupLabelOn = true;
    setTimeout(() => {
      this.keyupLabelOn = false;
    }, 450);
  }

  blinkKeydownLabel() {
    this.keydownLabelOn = true;
    setTimeout(() => {
      this.keydownLabelOn = false;
    }, 450);
  }

  ngOnInit() {
    var allModels = this.serviceService.config.uri.allModels;
    this.http.get(allModels).subscribe(data =>{
      this.Measures = data;
      console.log(this.Measures);
    });
//     this.Measures = [{  
//    "name":"demo_accu",
//    "type":"griffin",
//    "process.type":"batch",
//    "owner":"test",
//    "data.sources":[  
//       {  
//          "name":"source",
//          "connectors":[  
//             {  
//                "name":"source1513317492171",
//                "type":"HIVE",
//                "version":"1.2",
//                "data.unit":"2day",
//                "config":{  
//                   "database":"default",
//                   "table.name":"demo_src",
//                   "where":"dt=#YYYYMMdd# AND hour=#HH#"
//                },
//                "predicates":[  
//                   {  
//                      "type":"file.exist",
//                      "config":{  
//                         "root.path":"hdfs:///griffin/demo_src",
//                         "path":"/dt=#YYYYMMdd#/hour=#HH#/_DONE"
//                      }
//                   }
//                ]
//             }
//          ]
//       },
//       {  
//          "name":"target",
//          "connectors":[  
//             {  
//                "name":"target1513317499033",
//                "type":"HIVE",
//                "version":"1.2",
//                "data.unit":"1hour",
//                "config":{  
//                   "database":"default",
//                   "table.name":"demo_tgt",
//                   "where":"dt=#YYYYMMdd# AND hour=#HH#"
//                },
//                "predicates":[  
//                   {  
//                      "type":"file.exist",
//                      "config":{  
//                         "root.path":"hdfs:///griffin/demo_src",
//                         "path":""
//                      }
//                   }
//                ]
//             }
//          ]
//       }
//    ],
//    "evaluateRule":{  
//       "rules":[  
//          {  
//             "dsl.type":"griffin-dsl",
//             "dq.type":"accuracy",
//             "rule":"source.id=target.id"
//          }
//       ]
//    }
// }]
    this.config={
      behaviour: 'drag',
      connect: true,
      start: [-10, 0],
      keyboard: true,  // same as [keyboard]="true"
      step: 1,
      pageSteps: 0,  // number of page steps, defaults to 10
      range: {
        min: -10,
        max: 0
      },
      pips:{
        mode: 'steps',
        density: 10,
        // values: 1,
        stepped: true
      }
    }
  }

  
  ngAfterViewChecked(){
    this.resizeWindow();
  }
}
