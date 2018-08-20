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

import {Component, OnInit, EventEmitter, Input, Output} from "@angular/core";
import * as $ from "jquery";

@Component({
  selector: "app-configuration",
  templateUrl: "./configuration.component.html",
  styleUrls: ["./configuration.component.css"]
})
export class ConfigurationComponent implements OnInit {
  @Output() event = new EventEmitter();
  @Input()
  data = {
    where: "",
    timezone: "",
    num: 1,
    timetype: "day",
    needpath: false,
    path: ""
  };
  @Input() location: string;

  constructor() {
  }

  num: number;
  path: string;
  where: string;
  needpath: boolean;
  selectedType: string;
  configuration = {
    where: "",
    timezone: "",
    num: 1,
    timetype: "day",
    needpath: false,
    path: ""
  };
  timetypes = ["day", "hour", "minute"];
  timetype: string;
  timezones = [
    "UTC-12(IDL)",
    "UTC-11(MIT)",
    "UTC-10(HST)",
    "UTC-9:30(MSIT)",
    "UTC-9(AKST)",
    "UTC-8(PST)",
    "UTC-7(MST)",
    "UTC-6(CST)",
    "UTC-5(EST)",
    "UTC-4(AST)",
    "UTC-3:30(NST)",
    "UTC-3(SAT)",
    "UTC-2(BRT)",
    "UTC-1(CVT)",
    "UTC(WET,GMT)",
    "UTC+1(CET)",
    "UTC+2(EET)",
    "UTC+3(MSK)",
    "UTC+3:30(IRT)",
    "UTC+4(META)",
    "UTC+4:30(AFT)",
    "UTC+5(METB)",
    "UTC+5:30(IDT)",
    "UTC+5:45(NPT)",
    "UTC+6(BHT)",
    "UTC+6:30(MRT)",
    "UTC+7(IST)",
    "UTC+8(EAT)",
    "UTC+8:30(KRT)",
    "UTC+9(FET)",
    "UTC+9:30(ACST)",
    "UTC+10(AEST)",
    "UTC+10:30(FAST)",
    "UTC+11(VTT)",
    "UTC+11:30(NFT)",
    "UTC+12(PSTB)",
    "UTC+12:45(CIT)",
    "UTC+13(PSTC)",
    "UTC+14(PSTD)"
  ];
  timezone: string;

  upward() {
    this.configuration = {
      where: this.where,
      timezone: this.timezone,
      num: this.num,
      timetype: this.timetype,
      needpath: this.needpath,
      path: this.path
    };
    this.event.emit(this.configuration);
  }

  ngOnInit() {
    this.where = this.data.where;
    this.timezone = this.data.timezone;
    this.num = this.data.num;
    this.timetype = this.data.timetype;
    this.needpath = this.data.needpath;
    this.path = this.data.path;
  }
}
