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
import { HttpClient} from '@angular/common/http';
import { Ng2SmartTableModule ,LocalDataSource} from 'ng2-smart-table';
import {DataTableModule} from "angular2-datatable";
import {ServiceService} from '../service/service.service';

import { DatePipe } from '@angular/common';
import { Router} from "@angular/router";
import * as $ from 'jquery';

@Component({
  selector: 'app-job',
  templateUrl: './job.component.html',
  providers:[ServiceService],
  styleUrls: ['./job.component.css']
})
export class JobComponent implements OnInit {
  // results:object[];
  allInstances:any;
  results:any;
  source:LocalDataSource;
  deletedBriefRow:object;
  jobName:string;
  public visible = false;
  public visibleAnimate = false;
  oldindex:number;


  deletedRow : object;
  sourceTable :string;
  targetTable :string;
  deleteId : string;
  deleteIndex:number;
  deleteGroup :string;
  deleteJob :string;


  
  constructor(private http:HttpClient,private router:Router,public serviceService:ServiceService) { };

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => this.visible = false, 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains('modal')) {
      this.hide();
    }
  }
  
  // resultData = [{"jobName":"i-BA-0-1504837194000","measureId":"22","groupName":"BA","targetPattern":"YYYYMMdd-HH","triggerState":"NORMAL","nextFireTime":1505875500000,"previousFireTime":1504864200000,"interval":"300","sourcePattern":"YYYYMMdd-HH","jobStartTime":"1504800000000"},{"jobName":"i-BA-0-1504837194000","measureId":"22","groupName":"BA","targetPattern":"YYYYMMdd-HH","triggerState":"NORMAL","nextFireTime":1505875500000,"previousFireTime":1504864200000,"interval":"300","sourcePattern":"YYYYMMdd-HH","jobStartTime":"1504800000000"},{"jobName":"i-BA-0-1504837194000","measureId":"22","groupName":"BA","targetPattern":"YYYYMMdd-HH","triggerState":"NORMAL","nextFireTime":1505875500000,"previousFireTime":1504864200000,"interval":"300","sourcePattern":"YYYYMMdd-HH","jobStartTime":"1504800000000"}];
  remove(row){
    this.visible = true;
    setTimeout(() => this.visibleAnimate = true, 100);
    this.deletedRow = row;
    this.deleteIndex = this.results.indexOf(row);
    this.deletedBriefRow = row;
    this.deleteGroup = row.groupName;
    this.deleteJob = row.jobName;
  }

  confirmDelete(){
    let deleteJob = this.serviceService.config.uri.deleteJob;
    let deleteUrl = deleteJob + '?group=' + this.deleteGroup + '&jobName=' + this.deleteJob;
    this.http.delete(deleteUrl).subscribe(data => {
      let deleteResult:any = data;
      console.log(deleteResult.code);
      if(deleteResult.code==206){
        var self = this;
        self.hide();
        setTimeout(function () {
          self.results.splice(self.deleteIndex,1);
          // self.source.load(self.results);
        },0);
      }
    },
    err =>{
        console.log('Error when deleting record');

    });
  };
  
  showInstances(row){
    if(row.showDetail){
        row.showDetail = !row.showDetail;     
      return;
    }
    let index  = this.results.indexOf(row);
    if (this.oldindex!=undefined &&this.oldindex != index){
        this.results[this.oldindex].showDetail = false;}
    let getInstances = this.serviceService.config.uri.getInstances;
    let getInstanceUrl = getInstances+ '?group=' + 'BA' + '&jobName=' + row.jobName +'&page='+'0'+'&size='+'200';
    this.http.get(getInstanceUrl).subscribe(data =>{      
        row.showDetail = !row.showDetail;     
        this.allInstances = data;   
        setTimeout(function(){
          // console.log($('.pagination'));
          $('.pagination').css("marginBottom","-10px");
        },0);

        // this.source = new LocalDataSource(this.allInstances);
        // this.source.load(this.allInstances);
    });
    this.oldindex = index;
  }

  intervalFormat(second){
     if(second<60)
         return (second + 's');
     else if(second<3600)
     {
         if(second%60==0)
             return(second / 60 + 'min');
         else 
             return((second - second % 60) / 60 + 'min'+second % 60 + 's');
     }
     else 
     {
         if(second%3600==0)
             return ( second / 3600 + 'h');
         else
         {
             second = (second - second % 3600) / 3600 + 'h';
             var s = second % 3600;
             return ( second + (s-s%60)/60+'min'+s%60+'s');
         }
     }
  }
  
  
  ngOnInit():void {

    var self = this;
    let allJobs = this.serviceService.config.uri.allJobs;
  	this.http.get(allJobs).subscribe(data =>{       
        this.results = Object.keys(data).map(function(index){
          let job = data[index];
          job.showDetail = false;
          job.interval = self.intervalFormat(job.interval);
          return job;
        });    
    });
   // this.results = this.resultData;

  };
}
