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
import {ServiceService} from '../../../service/service.service';
import { TREE_ACTIONS, KEYS, IActionMapping, ITreeOptions } from 'angular-tree-component';
import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService,ToasterContainerComponent} from 'angular2-toaster';
import * as $ from 'jquery';
import { HttpClient} from '@angular/common/http';
import { Router} from "@angular/router";


class node {
  name: string;
  id: number;
  children:object[];
  isExpanded:boolean;
  cols:Col[];
  parent:string;
};

class Rule{
  type:string;
  conditionGroup = [
    {
      'type':'where',
      'content':'',
      'chosen':false,
      'avaliable':true
    },
    {
      'type':'groupby',
      'content':'',
      'chosen':false,
      'avaliable':true
    },
    {
      'type':'having',
      'content':'',
      'chosen':false,
      'avaliable':false
    },
    {
      'type':'orderby',
      'content':'',
      'chosen':false,
      'avaliable':true
    },
    {
      'type':'limit',
      'content':'',
      'chosen':false,
      'avaliable':true
    }
  ];
}

class Col{
  name:string;
  type:string;
  comment:string;
  selected :boolean;
  isNum:boolean;
  isExpanded:boolean;
  // rules:string[];
  groupby:string;
  RE:string;
  newRules:Rule[];
  ruleLength = 0;
  constructor(name:string,type:string,comment:string,selected:boolean){
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.selected = false;
    this.isExpanded = false;
    this.groupby = '';
    this.RE = '';
    this.newRules = [
    ];
    
    var patt = new RegExp('int|double|float/i');
    if(patt.test(this.type)){
      this.isNum = true;
    }
    // this.rules = [];
  }
}

@Component({
  selector: 'app-pr',
  templateUrl: './pr.component.html',
  providers:[ServiceService],
  styleUrls: ['./pr.component.css']
})
export class PrComponent implements OnInit {

  currentStep = 1;
  firstCond = false;
  selection : Col[];
  selectedAll = false;
  rules = '';
  currentDB = '';
  currentTable = '';
  schemaCollection:Col[];
  totallen = 0;
  type = 'profiling';
  data:any;
  newMeasure = {
    "name": "",
    "process.type": "batch",
    "data.sources": [
      {
        "name": "source",
        "connectors": [
          {
            "type": "hive",
            "version": "1.2",
            "config": {
              "database": "",
              "table.name":""
            }
          }
        ]
      }
    ],
    "evaluateRule": {
      "rules": [
        {
          "dsl.type": "griffin-dsl",
          "dq.type": "profiling",
          "rule": ""
          // "details": {}
        }
      ]
    }
  };
  name:'';
  createResult :any;
  newCond:any;

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

  toggleSelectionCond(cond,condIndex,ruleIndex,item){
    cond.chosen = !cond.chosen;
    if(condIndex==1&&cond.chosen)
      item.newRules[ruleIndex].conditionGroup[2].avaliable = true;
    if(condIndex==1&&!cond.chosen){
      item.newRules[ruleIndex].conditionGroup[2].avaliable = false;
      item.newRules[ruleIndex].conditionGroup[2].chosen = false;
    }
  }

  toggleSelection (row) {
      row.selected = !row.selected;
      console.log(row);
      var idx = this.selection.indexOf(row);
      // is currently selected
      if (idx > -1) {
          this.selection.splice(idx, 1);
          this.selectedAll = false;
      }
      // is newly selected
      else {
          this.selection.push(row);
      }
  };

  toggleAll () {
    this.selectedAll = !this.selectedAll;
    this.selection = [];
    for(var i =0; i < this.schemaCollection.length; i ++){
      this.schemaCollection[i].selected = this.selectedAll;
      if (this.selectedAll) {
          this.selection.push(this.schemaCollection[i]);
      }
    }
  };

  transferRule(rule,col){
    switch(rule){
      case 'Total Count':
        return 'count(source.'+col.name+') ';
      case 'Distinct Count':
        return 'distinct count(source.'+col.name+') ';
      case 'Null Detection Count':
        return 'count(source.'+col.name+') where source.'+col.name+' is null';
      case 'Regular Expression Detection Count':
        return 'count(source.'+col.name+') where source.'+col.name+' like ';
      case 'Rule Detection Count':
        return 'count(source.'+col.name+') where source.'+col.name+' like ';
      case 'Maxium':
        return 'max(source.'+col.name+') ';
      case 'Minimum':
        return 'min(source.'+col.name+') ';
      case 'Median':
        return 'median(source.'+col.name+') ';
      case 'Average':
        return 'average(source.'+col.name+') ';
      case 'Enum Detection Count':
        return 'source.'+col.name+' group by source.'+col.name+'';
      // case 'Groupby Count':
      //   return 'source.'+col.name+' group by source.'+col.name+'';
      // case 'total count':
      //   return 'SELECT COUNT(*) FROM source';
      // case 'distinct count':
      //   return 'SELECT DISTINCT COUNT(source.'+col.name+') FROM source';
      // case 'null detection count':
      //   return 'SELECT COUNT(source.'+col.name+') FROM source WHERE source.'+col.name+' is null';
      // case 'regular expression detection count':
      //   return 'SELECT COUNT(source.'+col.name+') FROM source WHERE source.'+col.name+' like '+col.RE;
      // case 'rule detection count':
      //   return 'SELECT COUNT(source.'+col.name+') FROM source WHERE source.'+col.name+' like ';
      // case 'max':
      //   return 'SELECT max(source.'+col.name+') FROM source';
      // case 'min':
      //   return 'SELECT min(source.'+col.name+') FROM source';
      // case 'median':
      //   return 'SELECT median(source.'+col.name+') FROM source';
      // case 'avg':
      //   return 'SELECT average(source.'+col.name+') FROM source';
      // case 'enum detection group count':
      //   return 'source.'+col.name+' group by source.'+col.name+'';
      // case 'groupby count':
      //   return 'source.'+col.name+' group by source.'+col.name+' '+col.groupby;
    }
  }

  addCond(item,ruleIndex){  }

  addRule(item){
    item.ruleLength++;
    let newRule = new Rule();
    item.newRules.push(newRule);
  }

  removeRule(item,ruleIndex){
    item.ruleLength--;
    item.newRules[ruleIndex] = null;
  }

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
        for(let item of this.selection){
          this.totallen = this.totallen + item.newRules.length;
        }
        return (this.totallen > 0)
    } else if (step == 3) {
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
      // if (!form.valid) {
      //   this.toasterService.pop('error', 'Error!', 'please complete the form in this step before proceeding');
      //   return false;
      // }
      this.newMeasure = {
        "name": this.name,
        "process.type": "batch",
        "data.sources": [
          {
            "name": "source",
            "connectors": [
              {
                "type": "hive",
                "version": "1.2",
                "config": {
                  "database": this.currentDB,
                  "table.name":this.currentTable
                }
              }
            ]
          }
        ],
        "evaluateRule": {
          "rules": [
            {
              "dsl.type": "griffin-dsl",
              "dq.type": "profiling",
              "rule": ""
              // "details": {}
            }
          ]
        }
      };
     
      var self = this;
      var rule = '';
      for(let item of this.selection){
          for(let itemRule of item.newRules){
            console.log(self.transferRule(itemRule.type,item));
            if(itemRule.conditionGroup[0].chosen==true){
              let whereRule = self.transferRule(itemRule.type,item);
              for(let condition of itemRule.conditionGroup){
                if(condition.content!='')
                  whereRule = whereRule + condition.type + ' ' + condition.content + ',';
              }
              self.newMeasure.evaluateRule.rules.push({
                "dsl.type": "griffin-dsl",
                "dq.type": "profiling",
                "rule": whereRule,
                // "details": {}
              });
            }
            else {
              let normalRule = self.transferRule(itemRule.type,item);
              for(let condition of itemRule.conditionGroup){
                if(condition.content!='')
                  normalRule = normalRule + ' '+ condition.type + ' ' + condition.content + ',';
              }
              rule = rule + normalRule;
            }
          }
      }
      // this.newMeasure.evaluateRule.rules[0].rule = rule;
      self.newMeasure.evaluateRule.rules.push({
        "dsl.type": "griffin-dsl",
        "dq.type": "profiling",
        "rule": rule,
        // "details": {}
      });
      this.visible = true;
      setTimeout(() => this.visibleAnimate = true, 100);
  }

  save() {
    console.log(this.newMeasure);
    var addModels = this.servicecService.config.uri.addModels;
    this.http
    .post(addModels, this.newMeasure)
    .subscribe(data => {
        this.createResult = data;
        this.hide();
        this.router.navigate(['/measures']);
    },
    err => {
      console.log('Something went wrong!');
    });
  }

  // data: { [key: string]: Array<object>; } = {
  //   "default": [
  //       {
  //           "tableName": "ext",
  //           "dbName": "default",
  //           "owner": "hadoop",
  //           "createTime": 1488353464,
  //           "lastAccessTime": 0,
  //           "retention": 0,
  //           "sd": {
  //               "cols": [
  //                   {
  //                       "name": "id",
  //                       "type": "int",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "name",
  //                       "type": "string",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "age",
  //                       "type": "int",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   }
  //               ],
  //               "location": "hdfs://10.9.246.187/user/hive/ext",
  //           },
  //       },
  //       {
  //           "tableName": "ext1",
  //           "dbName": "default",
  //           "owner": "hadoop",
  //           "createTime": 1489382943,
  //           "lastAccessTime": 0,
  //           "retention": 0,
  //           "sd": {
  //               "cols": [
  //                   {
  //                       "name": "id",
  //                       "type": "int",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "name",
  //                       "type": "string",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "age",
  //                       "type": "int",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   }
  //               ],
  //               "location": "hdfs://10.9.246.187/user/hive/ext1",
  //           },
  //       }
  //   ],
  //   "griffin": [
  //       {
  //           "tableName": "avr_out",
  //           "dbName": "griffin",
  //           "owner": "hadoop",
  //           "createTime": 1493892603,
  //           "lastAccessTime": 0,
  //           "retention": 0,
  //           "sd": {
  //               "cols": [
  //                   {
  //                       "name": "id",
  //                       "type": "bigint",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "age",
  //                       "type": "int",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   },
  //                   {
  //                       "name": "desc",
  //                       "type": "string",
  //                       "comment": null,
  //                       "setName": true,
  //                       "setComment": false,
  //                       "setType": true
  //                   }
  //               ],
  //               "location": "hdfs://10.9.246.187/griffin/data/batch/avr_out",
  //           },
  //       }
  //   ],
  // };
  
  options: ITreeOptions = {
    displayField: 'name',
    isExpandedField: 'expanded',
    idField: 'id',
    actionMapping: {
      mouse: {
        click: (tree, node, $event) => {
          if (node.hasChildren) {
            this.currentDB = node.data.name;
            this.currentTable = '';
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          }
          else if(node.data.cols)
          {
            this.currentTable = node.data.name;
            this.currentDB = node.data.parent;
            this.schemaCollection = node.data.cols;
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

  constructor(toasterService: ToasterService,private http: HttpClient,private router:Router,public servicecService:ServiceService) {
    this.toasterService = toasterService;
    this.selection = [];
  };


  ngOnInit() {
    var allDataassets = this.servicecService.config.uri.dataassetlist;
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
    
  };
}
