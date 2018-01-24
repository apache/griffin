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
import { Component, OnInit, AfterViewChecked, ViewChild } from '@angular/core';
import { FormControl } from '@angular/forms';
import { FormsModule, Validator} from '@angular/forms';
import {ServiceService} from '../../../service/service.service';
// import { PatternValidator } from '@angular/forms';


import { TREE_ACTIONS, KEYS, IActionMapping, ITreeOptions } from 'angular-tree-component';
import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService} from 'angular2-toaster';
import * as $ from 'jquery';
import { HttpClient} from '@angular/common/http';
import { Router} from "@angular/router";
// import { TagInputModule } from 'ngx-chips';


class node {
  name: string;
  id: number;
  children:object[];
  isExpanded:boolean;
  cols:Col[];
  parent:string;
  location:string;
};
class Col{
  name:string;
  type:string;
  comment:string;
  selected :boolean;
  constructor(name:string,type:string,comment:string,selected:boolean){
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.selected = false;
  }
  getSelected(){
    return this.selected;
  }
  setSelected(selected){
    this.selected = selected;
  }
}

@Component({
  selector: 'app-ac',
  templateUrl: './ac.component.html',
  providers:[ServiceService],
  styleUrls: ['./ac.component.css']
})

export class AcComponent implements OnInit , AfterViewChecked {
  
  defaultValue:string;
  currentStep = 1;
  // grp = [];
  // showgrp:string;
  // finalgrp = [];
  desc:string;
  selection = [];
  selectedAll = false;
  selectedAllTarget = false;
  selectionTarget = [];
  map = [];
  mappings = [];
  matches = [];
  dataAsset = '';
  rules = '';
  currentDB = '';
  currentTable = '';
  currentDBTarget = '';
  currentTableTarget = '';
  schemaCollection:Col[];
  schemaCollectionTarget:Col[];
  matchFunctions = ['=', '!=', '>', '>=','<',"<="];
  data:any;
  currentDBTargetStr: string;
  currentDBstr: string;
  srcconfig = {
    "where":'',
    "num":1,
    "timetype":'day',
    "needpath":false,
    "path":''
  };
  tgtconfig = {
    "where":'',
    "num":1,
    "timetype":'day',
    "needpath":false,
    "path":''
  };
  srcdata = {
    "database":'',
    "table":'',
    "selection":[]
  }
  tgtdata = {
    "database":'',
    "table":'',
    "selection":[]
  }
  src_where: string;
  tgt_where: string;
  src_size: string;
  tgt_size: string;
  src_path: string;
  tgt_path: string;
  src_name: string;
  tgt_name: string;
  src_location: string;
  tgt_location: string;

  measureTypes = ['accuracy','validity','anomaly detection','publish metrics'];
  type = "accuracy";
  newMeasure = {
    "name":'',
    "measure.type":"griffin",
    "dq.type": "accuracy",
    "process.type": "batch",
    "owner":"",
    "description":"",
    // "group":[],
    "data.sources": [
    {
      "name": "source",
      "connectors": [
        { 
          "name":"",
          "type": "HIVE",
          "version": "1.2",
          "data.unit":"",
          "config":{
            "database":'',
            "table.name":'',
            "where":''
          },
          "predicates":[
            {
              "type":"file.exist",
              "config":{
                "root.path":"hdfs:///griffin/demo_src",
                "path":""
              }
            }
          ]
        }
      ]
    }, {
      "name": "target",
      "connectors": [
        {
          "name":"",
          "type": "HIVE",
          "version": "1.2",
          "data.unit":"",
          "config":{
            "database":'',
            "table.name":'',
            "where":''
          },
          "predicates":[
            {
              "type":"file.exist",
              "config":{
                "root.path":"hdfs:///griffin/demo_src",
                "path":""
              }
            }
          ]
        }
      ]
    }
    ],

    "evaluate.rule":{
        "rules": [
          {
            "dsl.type": "griffin-dsl",
            "dq.type": "accuracy",
            "name": "accuracy",
            "rule": ""
            // "details": {
            //   "source": "source",
            //   "target": "target",
            //   "miss.records": {
            //     "name": "miss.records",
            //     "persist.type": "record"
            //   },
            //   "accuracy": {
            //     "name": "accu",
            //     "persist.type": "metric"
            //   },
            //   "miss": "miss",
            //   "total": "total",
            //   "matched": "matched"
            // }
          }
        ]
    }
  };
  name:'';
  // evaluate.rule:any;
  // desc:'';
  // grp:'';
  owner = 'test';
  createResult :any;

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

  addMapping(x,i){   
    this.mappings[i] = x;
  }

  toggleSelection (row) {
    row.selected = !row.selected;
    var idx = this.selection.indexOf(row.name);
    // is currently selected
    if (idx > -1) {
      this.selection.splice(idx, 1);
      this.selectedAll = false;
    }
    // is newly selected
    else {
      this.selection.push(row.name);
    }
    if(this.selection.length == 3){
      this.selectedAll = true;
    }else{
      this.selectedAll = false;
    }
  };

  toggleSelectionTarget (row) {
    row.selected = !row.selected;
    var idx = this.selectionTarget.indexOf(row.name);
    // is currently selected
    if (idx > -1) {
      this.selectionTarget.splice(idx, 1);
      this.selectedAllTarget = false;
    }
    // is newly selected
    else {
      this.selectionTarget.push(row.name);
    }
    if(this.selectionTarget.length == 3){
      this.selectedAllTarget = true;
    }else{
      this.selectedAllTarget = false;
    }
    let l = this.selectionTarget.length;
    for(let i =0;i<l;i++){
      this.matches[i] = "=";
      // this.mappings[i] = this.currentDB + '.' + this.currentTable + '.' + row.name;
    }
      
  };

  toggleAll () {
    this.selectedAll = !this.selectedAll;
    this.selection = [];
    for(var i =0; i < this.schemaCollection.length; i ++){
      this.schemaCollection[i].selected = this.selectedAll;
      if (this.selectedAll) {
        this.selection.push(this.schemaCollection[i].name);
        this.matches[i] = "=";
      }
    }
  };

  toggleAllTarget () {
    this.selectedAllTarget = !this.selectedAllTarget;
    this.selectionTarget = [];
    for(var i =0; i < this.schemaCollectionTarget.length; i ++){
      this.schemaCollectionTarget[i].selected = this.selectedAllTarget;
      if (this.selectedAllTarget) {
        this.selectionTarget.push(this.schemaCollectionTarget[i].name);
      }
    }
  };

  next (form) {
    if(this.formValidation(this.currentStep)){
      this.currentStep++;
    }else{
      this.toasterService.pop('error','Error!','Please select at least one attribute!');
        return false;
    }
  }

  formValidation = function(step) {
    if (step == undefined) {
      step = this.currentStep;
    }
    if (step == 1) {
      return this.selection && this.selection.length > 0;
    } else if (step == 2) {
      return (this.selectionTarget && this.selectionTarget.length > 0)//at least one target is selected
      // && !((this.currentTable.name == this.currentTableTarget.name)&&(this.currentDB.name == this.currentDBTarget.name));//target and source should be different
    } else if (step == 3) {
        return this.selectionTarget && this.selectionTarget.length == this.mappings.length
          && this.mappings.indexOf('') == -1
    } else if (step == 4) {
      return true;
    } else if(step == 5){

    }
    return false;
  } 

  prev (form) {
    this.currentStep--;
  }
  goTo (i) {
    this.currentStep = i;
  }
  submit (form) {              
      // form.$setPristine();
      // this.finalgrp = [];
      if (!form.valid) {
        this.toasterService.pop('error', 'Error!', 'please complete the form in this step before proceeding');
        return false;
      }
      // for(let i=0;i<this.grp.length;i++){
      //   this.finalgrp.push(this.grp[i].value);
      // }
      // this.showgrp = this.finalgrp.join(",");
      var rule = '';
      this.newMeasure = {
        "name":this.name,
        "measure.type":"griffin",
        "dq.type": "accuracy",
        "process.type": "batch",
        "owner":this.owner,
        "description":this.desc,
        // "group":this.finalgrp,
        "data.sources": [
          {
            "name": "source",
            "connectors": [
              {
                "name":this.src_name,
                "type": "HIVE",
                "version": "1.2",
                "data.unit":this.src_size,
                "config":{
                  "database":this.currentDB,
                  "table.name":this.currentTable,
                  "where":this.src_where
                },
                "predicates":[
                  {
                    "type":"file.exist",
                    "config":{
                      "root.path":"hdfs:///griffin/demo_src",
                      "path":this.src_path
                    }
                  }
                ]
              }
            ]
          }, {
            "name": "target",
            "connectors": [
              {
                "name":this.tgt_name,
                "type": "HIVE",
                "version": "1.2",
                "data.unit":this.tgt_size,
                "config":{
                  "database":this.currentDBTarget,
                  "table.name":this.currentTableTarget,
                  "where":this.tgt_where
                },
                "predicates":[
                  {
                    "type":"file.exist",
                    "config":{
                      "root.path":"hdfs:///griffin/demo_src",
                      "path":this.tgt_path
                    }
                  }
                ]
              }
            ]
          }
        ],     
        "evaluate.rule":{
          "rules": [
            {
              "dsl.type": "griffin-dsl",
              "dq.type": "accuracy",
              "name": "accuracy",
              "rule": ""
                 // "details": {
                 //   "source": "source",
                 //   "target": "target",
                 //   "miss.records": {
                 //     "name": "miss.records",
                 //     "persist.type": "record"
                 //   },
                 //   "accuracy": {
                 //     "name": "accu",
                 //     "persist.type": "metric"
                 //   },
                 //   "miss": "miss",
                 //   "total": "total",
                 //   "matched": "matched"
                 // }
            }
          ]
        }
      };
      if(this.src_size.indexOf('0')==0){
        delete this.newMeasure['data.sources'][0]['connectors'][0]['data.unit'];
      }
      if(this.tgt_size.indexOf('0')==0){
        delete this.newMeasure['data.sources'][1]['connectors'][0]['data.unit'];
      }
      var mappingRule = function(src, tgt, matches) {
        var rules;
        rules = 'source.' + src  + matches + 'target.' + tgt
        return rules;
      }
      var self = this;
      var rules = this.mappings.map(function(item, i) {
        return mappingRule(item,self.selectionTarget[i], self.matches[i]);
      });
      rule = rules.join(" AND ");
      this.rules = rule;
      this.newMeasure['evaluate.rule'].rules[0].rule = rule;
      this.visible = true;
      setTimeout(() => this.visibleAnimate = true, 100);
  }

  save() {
    var addModels = this.serviceService.config.uri.addModels;
    this.http
    .post(addModels, this.newMeasure)
    .subscribe(data => {
        this.createResult = data;
        this.hide();
        this.router.navigate(['/measures']);
        // var self = this;
        // setTimeout(function () {
        //   self.hide();
        //   self.router.navigate(['/measures']);
        // },0)
    },
    err => {
      console.log('Something went wrong!');
    });
  }

  options: ITreeOptions = {
    displayField: 'name',
    isExpandedField: 'expanded',
    idField: 'id',
    actionMapping: {
      mouse: {
        click: (tree, node, $event) => {         
          if (node.hasChildren) {
            this.currentDB = node.data.name;
            this.currentDBstr = this.currentDB + '.';
            this.currentTable = '';
            this.selectedAll = false;
            this.schemaCollection = [];
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          }
          else if(node.data.cols)
          {
            this.currentTable = node.data.name;
            this.currentDB = node.data.parent;
            this.schemaCollection = node.data.cols;
            this.src_location = node.data.location;
            this.src_name = 'source' + new Date().getTime();
            this.selectedAll = false;
            this.selection = [];
            for(let row of this.schemaCollection){
              row.selected = false;
            }
          }
        }
      }
    },
    animateExpand: true,
    animateSpeed: 30,
    animateAcceleration: 1.2
  };

  targetOptions: ITreeOptions = {
    displayField: 'name',
    isExpandedField: 'expanded',
    idField: 'id',
    actionMapping: {
      mouse: {
        click: (tree, node, $event) => {
          if (node.hasChildren) {
            this.currentDBTarget = node.data.name;
            this.currentDBTargetStr = this.currentDBTarget + '.';
            this.currentTableTarget = '';
            this.selectedAllTarget = false;
            this.selectionTarget = [];
            this.schemaCollectionTarget = [];
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          }
          else if(node.data.cols)
          {
            this.currentTableTarget = node.data.name;
            this.currentDBTarget = node.data.parent;
            this.schemaCollectionTarget = node.data.cols;
            this.tgt_location = node.data.location;
            this.tgt_name = 'target' + new Date().getTime();
            this.selectedAllTarget = false;
            this.selectionTarget = [];
            for(let row of this.schemaCollectionTarget){
              row.selected = false;
            }
          }
        }
      }
    },
    animateExpand: true,
    animateSpeed: 30,
    animateAcceleration: 1.2
  };

  nodeList:object[];
  nodeListTarget:object[];
  constructor(toasterService: ToasterService,private http: HttpClient,private router:Router,public serviceService:ServiceService) {
    this.toasterService = toasterService;
  };
  
  onResize(event){
    this.resizeWindow();
  }
  
  srcAttr(evt){
    this.srcdata = evt;
    this.currentDB = evt.database;
    this.currentTable = evt.table;
    this.selection = evt.selection;
  }
  
  tgtAttr(evt){
    this.tgtdata = evt;
    this.currentDBTarget = evt.database;
    this.currentTableTarget = evt.table;
    this.selectionTarget = evt.selection;
  }

  getSrc(evt){
    this.srcconfig = evt;
    this.src_where = evt.where;
    this.src_size = evt.num + evt.timetype;
    this.src_path = evt.path;
  }

  getTgt(evt){
    this.tgtconfig = evt;
    this.tgt_where = evt.where;
    this.tgt_size = evt.num + evt.timetype;
    this.tgt_path = evt.path;
  }
  

  resizeWindow(){
    var stepSelection = '.formStep[id=step-' + this.currentStep + ']';
    $(stepSelection).css({
      height: window.innerHeight - $(stepSelection).offset().top
    });
    $('fieldset').height($(stepSelection).height() - $(stepSelection + '>.stepDesc').height() - $('.btn-container').height() - 130);
    $('.y-scrollable').css({
        // 'max-height': $('fieldset').height()- $('.add-dataset').outerHeight()
        'height': $('fieldset').height()
    });
  }
    
  ngOnInit() {
    var allDataassets = this.serviceService.config.uri.dataassetlist;
    this.http.get(allDataassets).subscribe(data =>{
      this.nodeList = new Array();
      let i = 1;
      this.data = data;
      for (let db in this.data) {
        let new_node = new node();
        new_node.name = db;
        new_node.id = i;
        new_node.isExpanded = true;
        i++;
        new_node.children = new Array();
        for(let i = 0;i<this.data[db].length;i++){
          let new_child = new node();
          new_child.name = this.data[db][i]['tableName'];
          new_node.children.push(new_child);
          new_child.isExpanded = false;
          new_child.location = this.data[db][i]['sd']['location'];
          new_child.parent = db;
          new_child.cols = Array<Col>();
          for(let j = 0;j<this.data[db][i]['sd']['cols'].length;j++){
              let new_col = new Col(this.data[db][i]['sd']['cols'][j].name,
              this.data[db][i]['sd']['cols'][j].type,
              this.data[db][i]['sd']['cols'][j].comment,false);
              new_child.cols.push(new_col);
          }
        }
        this.nodeList.push(new_node);
    }
    this.nodeListTarget = JSON.parse(JSON.stringify(this.nodeList));
    });
    this.src_size = '1day';
    this.tgt_size = '1day';
  };

  ngAfterViewChecked(){
    this.resizeWindow();
  }
}
