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
import { Component, OnInit, OnChanges, SimpleChanges, OnDestroy, AfterViewInit, NgZone } from "@angular/core";
import { ChartService } from "../../service/chart.service";
import { ServiceService } from "../../service/service.service";
import { Router, ActivatedRoute, ParamMap } from "@angular/router";
import "rxjs/add/operator/switchMap";
import { HttpClient } from "@angular/common/http";
import * as $ from "jquery";

@Component({
  selector: "app-detail-metric",
  templateUrl: "./detail-metric.component.html",
  styleUrls: ["./detail-metric.component.css"],
  providers: [ChartService, ServiceService]
})
export class DetailMetricComponent implements OnInit {
  constructor(
    public chartService: ChartService,
    private route: ActivatedRoute,
    private router: Router,
    private http: HttpClient,
    private zone: NgZone,
    public serviceService: ServiceService
  ) {}
  selectedMeasure: string;
  chartOption: {};
  data: any;
  currentJob: string;
  finalData: any;
  metricName: string;
  size = 300;
  offset = 0;

  ngOnInit() {
    this.currentJob = this.route.snapshot.paramMap.get("name");
    var self = this;
    var metricdetail = self.serviceService.config.uri.metricdetail;
    var metricDetailUrl =
      metricdetail +
      "?metricName=" +
      this.currentJob +
      "&size=" +
      this.size +
      "&offset=" +
      this.offset;
    this.http.get(metricDetailUrl).subscribe(
      data => {
        var metric = {
          name: "",
          timestamp: 0,
          dq: 0,
          details: []
        };
        this.data = data;
        if (this.data) {
          metric.name = this.data[0].name;
          metric.timestamp = this.data[0].tmst;
          metric.dq =
            this.data[0].value.matched / this.data[0].value.total * 100;
          metric.details = JSON.parse(JSON.stringify(this.data));
        }
        this.chartOption = this.chartService.getOptionBig(metric);
        $("#bigChartDiv").height(window.innerHeight - 120 + "px");
        $("#bigChartDiv").width(window.innerWidth - 400 + "px");
        $("#bigChartContainer").show();
      },
      err => {
        console.log("Error occurs when connect to elasticsearh!");
      }
    );
  }

  onResize(event) {
    this.resizeTreeMap();
  }

  resizeTreeMap() {
    $("#bigChartDiv").height($("#mainWindow").height());
    $("#bigChartDiv").width($("#mainWindow").width());
  }

  getData(metricName) {}
}