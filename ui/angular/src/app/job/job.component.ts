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
import { Component, OnInit } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { DataTableModule } from "angular2-datatable";
import { ServiceService } from "../service/service.service";
import { DatePipe } from "@angular/common";
import { Router } from "@angular/router";
import { ToasterModule, ToasterService, ToasterConfig } from "angular2-toaster";
import * as $ from "jquery";

@Component({
  selector: "app-job",
  templateUrl: "./job.component.html",
  providers: [ServiceService],
  styleUrls: ["./job.component.css"]
})
export class JobComponent implements OnInit {
  allInstances: any;
  results: any;
  jobName: string;
  public visible = false;
  public visibleAnimate = false;
  oldindex: number;
  deletedRow: object;
  sourceTable: string;
  targetTable: string;
  deleteId: string;
  deleteIndex: number;
  private toasterService: ToasterService;

  constructor(
    toasterService: ToasterService,
    private http: HttpClient,
    private router: Router,
    public serviceService: ServiceService
  ) {
    this.toasterService = toasterService;
  }

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => (this.visible = false), 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains("modal")) {
      this.hide();
    }
  }

  remove(row) {
    $("#save").removeAttr("disabled");
    this.visible = true;
    setTimeout(() => (this.visibleAnimate = true), 100);
    this.deletedRow = row;
    this.deleteIndex = this.results.indexOf(row);
    this.deleteId = row.jobId;
  }

  show(row) {
    var curjob = row.jobName;
    this.router.navigate(['/detailed/'+curjob]);
  }

  confirmDelete() {
    let deleteJob = this.serviceService.config.uri.deleteJob;
    let deleteUrl = deleteJob + "/" + this.deleteId;
    $("#save").attr("disabled", "true");
    this.http.delete(deleteUrl).subscribe(
      data => {
        let self = this;
        self.hide();
        setTimeout(function() {
          self.results.splice(self.deleteIndex, 1);
        }, 0);
      },
      err => {
        this.toasterService.pop("error", "Error!", "Failed to delete job!");
        console.log("Error when deleting job");
      }
    );
  }

  showInstances(row) {
    if (row.showDetail) {
      row.showDetail = !row.showDetail;
      return;
    }
    let index = this.results.indexOf(row);
    if (this.oldindex != undefined && this.oldindex != index) {
      this.results[this.oldindex].showDetail = false;
    }
    let getInstances = this.serviceService.config.uri.getInstances;
    let getInstanceUrl = getInstances + "?jobId=" + row.jobId + "&page=" + "0" + "&size=" + "200";
    this.http.get(getInstanceUrl).subscribe(data => {
      row.showDetail = !row.showDetail;
      this.allInstances = data;
      setTimeout(function() {
        $(".pagination").css("marginBottom", "-10px");
      }, 0);
    });
    this.oldindex = index;
  }

  ngOnInit(): void {
    var self = this;
    let allJobs = this.serviceService.config.uri.allJobs;
    this.http.get(allJobs).subscribe(data => {
      let trans = Object.keys(data).map(function(index) {
        let job = data[index];
        job.showDetail = false;
        return job;
      });
      this.results = Object.assign([],trans).reverse();
    });   
  }
}
