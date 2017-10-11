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
import { FormControl } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { MaxLengthValidator } from '@angular/forms';
import { NgControlStatus ,Validators} from '@angular/forms';
import { PatternValidator } from '@angular/forms';
// import {MdDatepickerModule} from '@angular/material';
import {MatDatepickerModule} from '@angular/material';
import {ServiceService} from '../../service/service.service';


import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {ToasterModule, ToasterService, ToasterConfig} from 'angular2-toaster';
import * as $ from 'jquery';
import  {HttpClient,HttpParams} from '@angular/common/http';
import  {Router} from "@angular/router";

@Component({
  selector: 'app-create-job',
  templateUrl: './create-job.component.html',
  providers:[ServiceService],
  styleUrls: ['./create-job.component.css']
})
export class CreateJobComponent implements OnInit {

  constructor(toasterService: ToasterService,private http: HttpClient,private router:Router,public servicecService:ServiceService) {
    this.toasterService = toasterService;
  };

  public toasterconfig : ToasterConfig =
        new ToasterConfig({
            showCloseButton: true,
            tapToDismiss: false,
            timeout: 0
        });

  currentStep = 1;
  Times = ['seconds','minutes','hours'];
  timeType = 'seconds';
  isOpen = false;
  maskOpen = false;

  hourDetail = '00';
  minuteDetail = '00';
  secondDetail = '00';
  timeDetail = '00:00:00';
  periodTime :number;
  StartTime = '';
  sourcePat :'';
  targetPat :'';
  createResult = '';
  jobStartTime : any;

  Measures:object;

  measure:string;
  measureid:string;
  ntAccount = 0;
  newJob={
        "sourcePattern":'',
        "targetPattern":'',
        "jobStartTime":0,
        "interval":'',
        "groupName":'',
      }

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

  changeTime(min,max,increase,time,type){
  	time = parseInt(time);
  	if(increase){
          if(time==max)
              time = min;
          else time = time + 1;
      }
      else{
          if(time==min)
              time = max;
          else time = time - 1;
      }
      if(time < 10)
          time = '0' + time;
      if(type==1)
          this.hourDetail = time;
      else if(type==2)
          this.minuteDetail = time;
      else
          this.secondDetail = time;
      this.timeDetail = this.hourDetail+':'+this.minuteDetail+':'+this.secondDetail;
  }

  showTime(){
  	this.isOpen = !this.isOpen;
    this.maskOpen = !this.maskOpen;
  }

  close(){
  	this.isOpen = false;
    this.maskOpen = false;
  }

  prev(form){
  	history.back();
  }

  submit (jobForm) {
      // jobForm.markAsPristine();
      var period;
      if(this.timeType=='minutes')
          period = this.periodTime *60;
      else if(this.timeType=='hours')
          period = this.periodTime * 3600;
      else period = this.periodTime;
      var rule = '';
      var time :number;
      if(this.jobStartTime){
        var year = this.jobStartTime.getFullYear();
        var month = this.jobStartTime.getMonth() + 1;
        var day = this.jobStartTime.getDate();
        var startTime = year +'-'+ month + '-'+ day + ' '+ this.timeDetail;
      }

      time = Date.parse(startTime);
      if(isNaN(time)){
         this.toasterService.pop('error','Error!','Please input the right format of start time');
          return false;
      }
      if (!jobForm.valid) {
        this.toasterService.pop('error', 'Error!', 'Please complete the form!');
        return false;
      }

      this.newJob={
        "sourcePattern":this.sourcePat,
        "targetPattern":this.targetPat,
        "jobStartTime":time,
        "interval":period,
        "groupName":'BA',
      },
      this.visible = true;
      setTimeout(() => this.visibleAnimate = true, 100);
  }
  save() {
  	var date = new Date();
  	var datastr = date.toString();
    var month = date.getMonth()+1;
    var timestamp = Date.parse(datastr);
    var jobName = this.measure + '-BA-' + this.ntAccount + '-' + timestamp;
    var addJobs = this.servicecService.config.uri.addJobs;
    var newJob = addJobs + '?group=' + this.newJob.groupName + '&jobName=' + jobName + '&measureId=' + this.measureid;
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
    $('fieldset').height($(stepSelection).height() - $(stepSelection + '>.stepDesc').height() - $('.btn-container').height() - 80);
    $('.y-scrollable').css({
        'max-height': $('fieldset').height()- $('.add-dataset').outerHeight()
    });
    $('#data-asset-pie').css({
        height: $('#data-asset-pie').parent().width(),
        width: $('#data-asset-pie').parent().width()
    });
  }

  setHeight(){
  	$('#md-datepicker-0').height(250);
  }

  ngOnInit() {
    var allModels = this.servicecService.config.uri.allModels;
    this.http.get(allModels).subscribe(data =>{
      this.Measures = data;
      this.measure = this.Measures[0].name;
      this.measureid = this.Measures[0].id;
    });
  }

}
