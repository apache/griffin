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
import { FormsModule, Validator} from '@angular/forms';
import {ServiceService} from '../../../service/service.service';
// import { PatternValidator } from '@angular/forms';


import { TREE_ACTIONS, KEYS, IActionMapping, ITreeOptions } from 'angular-tree-component';
import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService} from 'angular2-toaster';
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

export class AcComponent implements OnInit {

  currentStep = 1;
  org:string;
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

  measureTypes = ['accuracy','validity','anomaly detection','publish metrics'];
  type = 'accuracy';
  newMeasure = {
    "name":'',
    "process.type": "batch",
    "owner":"",
    "description":"",
    "organization":"",
    "data.sources": [
    {
      "name": "source",
      "connectors": [
        {
          "type": "HIVE",
          "version": "1.2",
          "config":{
            "database":'',
            "table.name":'',
          }
        }
      ]
    }, {
      "name": "target",
      "connectors": [
        {
          "type": "HIVE",
          "version": "1.2",
          "config":{
            "database":'',
            "table.name":'',
          }
        }
      ]
    }
    ],

    "evaluateRule":{
        "rules": [
          {
            "dsl.type": "griffin-dsl",
            "dq.type": "accuracy",
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
  evaluateRule:any;
  // desc:'';
  // org:'';
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
      let l = this.selectionTarget.length;
      for(let i =0;i<l;i++)
        this.matches[i] = "=";
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
      if (!form.valid) {
        this.toasterService.pop('error', 'Error!', 'please complete the form in this step before proceeding');
        return false;
      }
      var rule = '';
      this.newMeasure = {
         "name":this.name,
         "process.type": "batch",
         "owner":this.owner,
         "description":this.desc,
         "organization":this.org,
         "data.sources": [
         {
           "name": "source",
           "connectors": [
             {
               "type": "HIVE",
               "version": "1.2",
               "config":{
                 "database":this.currentDB,
                 "table.name":this.currentTable,
               }
             }
           ]
         }, {
           "name": "target",
           "connectors": [
             {
               "type": "HIVE",
               "version": "1.2",
               "config":{
                 "database":this.currentDBTarget,
                 "table.name":this.currentTableTarget,
               }
             }
           ]
         }
         ],
     
         "evaluateRule":{
             "rules": [
               {
                 "dsl.type": "griffin-dsl",
                 "dq.type": "accuracy",
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
      var mappingRule = function(src, tgt, matches) {
          var rules;
          rules = 'source.' + src  + matches + 'target.' + tgt
          return rules;
      }
      var self = this;
      var rules = this.selectionTarget.map(function(item, i) {
          return mappingRule(self.selection[i], item, self.matches[i]);
      });
      rule = rules.join(" AND ");
      this.rules = rule;
      this.newMeasure.evaluateRule.rules[0].rule = rule;
      // for(var i =0; i < this.selectionTarget.length; i ++){
      //   this.newMeasure.mappings.push({target:this.selectionTarget[i],
      //                   // src:this.mappings[i],
      //                   matchMethod: this.matches[i]});
      // }
      this.visible = true;
      setTimeout(() => this.visibleAnimate = true, 100);
  }

  save() {

    var addModels = this.servicecService.config.uri.addModels;

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

  targetOptions: ITreeOptions = {
    displayField: 'name',
    isExpandedField: 'expanded',
    idField: 'id',
    actionMapping: {
      mouse: {
        click: (tree, node, $event) => {
          if (node.hasChildren) {
            this.currentDBTarget = node.data.name;
            this.currentTableTarget = '';
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          }
          else if(node.data.cols)
          {
            this.currentTableTarget = node.data.name;
            this.currentDBTarget = node.data.parent;
            this.schemaCollectionTarget = node.data.cols;
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
  };
  
  onResize(event){
    console.log("Width: " + event.target.innerWidth);
   this.resizeWindow();
  }

  resizeWindow(){
      var stepSelection = '.formStep[id=step-' + this.currentStep + ']';
                    $(stepSelection).css({
                        height: window.innerHeight - $(stepSelection).offset().top
                    });
  }

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
