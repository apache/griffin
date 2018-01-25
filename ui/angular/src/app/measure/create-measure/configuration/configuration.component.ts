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

import { Component, OnInit, EventEmitter, Input, Output  } from '@angular/core';

@Component({
  selector: 'app-configuration',
  templateUrl: './configuration.component.html',
  styleUrls: ['./configuration.component.css']
})
export class ConfigurationComponent implements OnInit {
  @Output() event = new EventEmitter();
  @Input() data = {
    "where":'',
    "num":1,
    "timetype":'day',
    "needpath":false,
    "path":''
  };
  @Input() location:string;

  constructor() { }
  num:number;
  path:string;
  where:string;
  needpath:boolean;
  selectedType: string;
  configuration = {
  	"where":'',
  	"num":1,
    "timetype":'day',
    "needpath":false,
  	"path":''
  }
  timetypes = ["day","hour","minute"];
  timetype :string;

  upward(){
    this.configuration = {
      "where":this.where,
      "num":this.num,
      "timetype":this.timetype,
      "needpath":this.needpath,
      "path":this.path
    }
    this.event.emit(this.configuration);
  }

  ngOnInit() {
    this.where = this.data.where;
    this.num = this.data.num;
    this.timetype = this.data.timetype;
    this.needpath = this.data.needpath;
    this.path = this.data.path;
  }
 
}
