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
import {DataTableModule} from "angular2-datatable";
import { AfterViewInit, ElementRef} from '@angular/core';
import { AngularMultiSelectModule } from 'angular2-multiselect-dropdown/angular2-multiselect-dropdown';



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
  // conditionGroup = [
  //   {
  //     'type':'where',
  //     'content':'',
  //     'chosen':false,
  //     'avaliable':true
  //   },
  //   {
  //     'type':'groupby',
  //     'content':'',
  //     'chosen':false,
  //     'avaliable':true
  //   },
  //   {
  //     'type':'having',
  //     'content':'',
  //     'chosen':false,
  //     'avaliable':false
  //   },
  //   {
  //     'type':'orderby',
  //     'content':'',
  //     'chosen':false,
  //     'avaliable':true
  //   },
  //   {
  //     'type':'limit',
  //     'content':'',
  //     'chosen':false,
  //     'avaliable':true
  //   }
  // ];
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
  rules:any;
  newRules:Rule[];
  ruleLength = 0;
  constructor(name:string,type:string,comment:string,selected:boolean){
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.selected = false;
    this.isExpanded = false;
    this.groupby = '';
    this.rules = [];
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
  
  transrule = [];
  transenumrule = [];
  transnullrule = [];
  showrule = false;
  dropdownList = {};
  selectedItems = {};
  dropdownSettings = {};
  currentStep = 1;
  firstCond = false;
  mouseover = false;
  selection : Col[];
  selectedAll = false;
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
    this.transrule = [];
    this.transenumrule = [];
    this.transnullrule = [];
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains('modal')) {
      this.hide();
    }
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
  }

  // toggleSelectionCond(cond,condIndex,ruleIndex,item){
  //   cond.chosen = !cond.chosen;
  //   if(condIndex==1&&cond.chosen)
  //     item.newRules[ruleIndex].conditionGroup[2].avaliable = true;
  //   if(condIndex==1&&!cond.chosen){
  //     item.newRules[ruleIndex].conditionGroup[2].avaliable = false;
  //     item.newRules[ruleIndex].conditionGroup[2].chosen = false;
  //   }
  // }
  
  setDropdownList(){
    if(this.selection){
      for(let item of this.selection){
        if(item.isNum == true){
          this.dropdownList[item.name] = [
                              {"id":1,"itemName":"Null Count","category": "Simple Statistics"},
                              {"id":2,"itemName":"Distinct Count","category": "Simple Statistics"},
                              {"id":3,"itemName":"Total Count","category": "Summary Statistics"},
                              {"id":4,"itemName":"Maximum","category": "Summary Statistics"},
                              {"id":5,"itemName":"Minimum","category": "Summary Statistics"},
                              {"id":6,"itemName":"Average","category": "Summary Statistics"},
                              // {"id":7,"itemName":"Median","category": "Summary Statistics"},
                              // {"id":8,"itemName":"Rule Detection Count","category": "Advanced Statistics"},
                              {"id":9,"itemName":"Enum Detection Count","category": "Advanced Statistics"}
                            ];
        }else{
          this.dropdownList[item.name] = [
                              {"id":1,"itemName":"Null Count","category": "Simple Statistics"},
                              {"id":2,"itemName":"Distinct Count","category": "Simple Statistics"},
                              {"id":3,"itemName":"Total Count","category": "Summary Statistics"},
                              // {"id":8,"itemName":"Rule Detection Count","category": "Advanced Statistics"},
                              {"id":9,"itemName":"Enum Detection Count","category": "Advanced Statistics"},
                              // {"id":10,"itemName":"Regular Expression Detection Count","category": "Advanced Statistics"}
                            ];
        }
      }
    }
  }

  toggleSelection (row) {
    row.selected = !row.selected;
    // console.log(row);
    var idx = this.selection.indexOf(row);
    // is currently selected
    if (idx > -1) {
        this.selection.splice(idx, 1);
        this.selectedAll = false;       
        this.selectedItems[row.name] = [];
    }
    // is newly selected
    else {
        this.selection.push(row);
    }
    this.setDropdownList();
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
    this.setDropdownList();
  };

  transferRule(rule,col){
    switch(rule){
      case 'Total Count':
        return 'count(source.`'+col.name+'`) AS `'+col.name+'-count`';
      case 'Distinct Count':
        return 'approx_count_distinct(source.`'+col.name+'`) AS `'+col.name+'-distcount`';
      case 'Null Count':
        return 'count(source.`'+col.name+'`) AS `'+col.name+'-nullcount'+'` WHERE source.`'+col.name+'` IS NULL';
      // case 'Regular Expression Detection Count':
      //   return 'count(source.`'+col.name+'`) where source.`'+col.name+'` LIKE ';
      // case 'Rule Detection Count':
      //   return 'count(source.`'+col.name+'`) where source.`'+col.name+'` LIKE ';
      case 'Maximum':
        return 'max(source.`'+col.name+'`) AS `'+col.name+'-max`';
      case 'Minimum':
        return 'min(source.`'+col.name+'`) AS `'+col.name+'-min`';
      // case 'Median':
      //   return 'median(source.`'+col.name+'`) ';
      case 'Average':
        return 'avg(source.`'+col.name+'`) AS `'+col.name+'-average`';
      case 'Enum Detection Count':
        return 'source.`'+col.name+'`,count(*) AS `'+col.name+'-enum` GROUP BY source.`'+col.name+'`';
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
      var len = 0;
      console.log(this.selection);
      console.log(this.selectedItems);
      for(let key in this.selectedItems){
         len += this.selectedItems[key].length;
      }
      return (this.selection.length == len) ? true :false;
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
            // {
            //   "dsl.type": "griffin-dsl",
            //   "dq.type": "profiling",
            //   "rule": ""
              // "details": {}
            // }
          ]
        }
    };   
    this.getGrouprule();
    this.visible = true;
    setTimeout(() => this.visibleAnimate = true, 100);
  }
  
  getRule(trans){
    var self = this;
    var rule = '';
    for(let i of trans){
       rule = rule + i + ',';
    }
    rule = rule.substring(0,rule.lastIndexOf(','));
    self.newMeasure.evaluateRule.rules.push({
      "dsl.type": "griffin-dsl",
      "dq.type": "profiling",
      "rule": rule,
      // "details": {}
    });
  }

  save() {
    console.log(this.newMeasure);
    var addModels = this.serviceService.config.uri.addModels;
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

  constructor(private elementRef:ElementRef,toasterService: ToasterService,private http: HttpClient,private router:Router,public serviceService:ServiceService) {
    this.toasterService = toasterService;
    this.selection = [];
  };
  
  // onItemSelect(item){
  //   this.getRule();
  // }
  
  getGrouprule(){
    var selected = {name: ''};
    var value = '';
    for(let key in this.selectedItems){
      selected.name = key;
      for(let i = 0;i<this.selectedItems[key].length;i++){
        var originrule = this.selectedItems[key][i].itemName;
        if(originrule == 'Enum Detection Count'){
          value = this.transferRule(originrule,selected);
          this.transenumrule.push(value);
        }else if(originrule == 'Null Count'){
          value = this.transferRule(originrule,selected);
          this.transnullrule.push(value);
        }else{ 
          value = this.transferRule(originrule,selected);      
          this.transrule.push(value);
        }
      }  
    }
    this.getRule(this.transenumrule);
    this.getRule(this.transnullrule);
    this.getRule(this.transrule);
  }

  // OnItemDeSelect(item){
  //   this.getRule();
  // }

  confirmAdd(){
    document.getElementById('rule').style.display = 'none';
  }

  showRule(){
    document.getElementById('showrule').style.display = '';
    document.getElementById('notshowrule').style.display = 'none';
  }

  back () {
    document.getElementById('showrule').style.display = 'none';
    document.getElementById('notshowrule').style.display = '';
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
    this.dropdownSettings = { 
                                  singleSelection: false, 
                                  text:"Select Rule",
                                  // selectAllText:'Select All',
                                  // unSelectAllText:'UnSelect All',
                                  // badgeShowLimit: 5,
                                  enableCheckAll: false,
                                  enableSearchFilter: true,
                                  classes: "myclass",
                                  groupBy: "category"
                                };  


    
  };
}
