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
import { FormControl } from "@angular/forms";
import { FormsModule } from "@angular/forms";
import { ServiceService } from "../../../service/service.service";
import { TREE_ACTIONS, KEYS, IActionMapping, ITreeOptions } from "angular-tree-component";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { ToasterModule, ToasterService, ToasterContainerComponent } from "angular2-toaster";
import * as $ from "jquery";
import { HttpClient } from "@angular/common/http";
import { Router } from "@angular/router";
import { DataTableModule } from "angular2-datatable";
import { AfterViewChecked, ElementRef } from "@angular/core";
import { AngularMultiSelectModule } from "angular2-multiselect-dropdown/angular2-multiselect-dropdown";
import { ConfigurationComponent } from "../configuration/configuration.component";

class node {
  name: string;
  id: number;
  children: object[];
  isExpanded: boolean;
  cols: Col[];
  parent: string;
  location: string;
}

class Rule {
  type: string;
}

class Col {
  name: string;
  type: string;
  comment: string;
  selected: boolean;
  isNum: boolean;
  isExpanded: boolean;
  // rules:string[];
  groupby: string;
  RE: string;
  rules: any;
  newRules: Rule[];
  ruleLength = 0;
  constructor(name: string, type: string, comment: string, selected: boolean) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.selected = false;
    this.isExpanded = false;
    this.groupby = "";
    this.rules = [];
    this.RE = "";
    this.newRules = [];

    var patt = new RegExp("int|double|float/i");
    if (patt.test(this.type)) {
      this.isNum = true;
    }
    // this.rules = [];
  }
}

@Component({
  selector: "app-pr",
  templateUrl: "./pr.component.html",
  providers: [ServiceService],
  styleUrls: ["./pr.component.css"]
})
export class PrComponent implements AfterViewChecked, OnInit {
  noderule = [];
  transrule = [];
  transenumrule = [];
  transnullrule = [];
  showrule = false;
  dropdownList = {};
  selectedItems = {};
  allRules = {};
  dropdownSettings = {};
  currentStep = 1;
  firstCond = false;
  mouseover = false;
  selection: Col[];
  selectedAll = false;
  currentDB = "";
  currentTable = "";
  schemaCollection: Col[];
  totallen = 0;
  type = "profiling";
  data: any;
  desc: string;
  owner = "test";
  currentDBstr: string;
  timezone = "";
  ruledesc = "";
  newMeasure = {
    name: "",
    "measure.type": "griffin",
    "dq.type": "PROFILING",
    "process.type": "BATCH",
    owner: "",
    description: "",
    "rule.description": {
      details:[]
      },
    // "group":[],
    "data.sources": [
      {
        name: "source",
        connectors: [
          {
            name: "",
            type: "HIVE",
            version: "1.2",
            "data.unit": "",
            "data.time.zone": "",
            config: {
              database: "",
              "table.name": "",
              where: ""
            },
            predicates: [
              {
                type: "file.exist",
                config: {
                  "root.path": '',
                  path: ""
                }
              }
            ]
          }
        ]
      }
    ],
    "evaluate.rule": {
      rules: [
        // {
        //   "dsl.type": "griffin-dsl",
        //   "dq.type": "profiling",
        //   rule: "",
        //   name: "",
        // }
      ]
    }
  };
  name: "";
  createResult: any;
  newCond: any;
  srclocation: string;
  srcname: string;
  config = {
    where: "",
    timezone: "",
    num: 1,
    timetype: "day",
    needpath: false,
    path: ""
  };
  where: string;
  size: string;
  path: string;
  location: string;
  needpath: boolean;

  private toasterService: ToasterService;
  public visible = false;
  public visibleAnimate = false;

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => (this.visible = false), 300);
    this.transrule = [];
    this.transenumrule = [];
    this.transnullrule = [];
    this.noderule = [];
    $("#save").removeAttr("disabled");
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains("modal")) {
      this.hide();
    }
  }

  onResize(event) {
    this.resizeWindow();
  }

  resizeWindow() {
    var stepSelection = ".formStep";
    $(stepSelection).css({
      // height: window.innerHeight - $(stepSelection).offset().top - $('#footerwrap').outerHeight()
      height: window.innerHeight - $(stepSelection).offset().top
    });
    $("fieldset").height(
      $(stepSelection).height() -
        $(stepSelection + ">.stepDesc").height() -
        $(".btn-container").height() -
        130
    );
    $(".y-scrollable").css({
      // 'max-height': $('fieldset').height()- $('.add-dataset').outerHeight()
      height: $("fieldset").height()
    });
  }

  setDropdownList() {
    if (this.selection) {
      for (let item of this.selection) {
        if (item.isNum == true) {
          this.dropdownList[item.name] = [
            { id: 1, itemName: "Null Count", category: "Simple Statistics" },
            { id: 2, itemName: "Distinct Count", category: "Simple Statistics" },
            { id: 3, itemName: "Total Count", category: "Summary Statistics" },
            { id: 4, itemName: "Maximum", category: "Summary Statistics" },
            { id: 5, itemName: "Minimum", category: "Summary Statistics" },
            { id: 6, itemName: "Average", category: "Summary Statistics" },
            // {"id":7,"itemName":"Median","category": "Summary Statistics"},
            // {"id":8,"itemName":"Rule Detection Count","category": "Advanced Statistics"},
            { id: 9, itemName: "Enum Detection Top5 Count", category: "Advanced Statistics" }
          ];
        } else {
          this.dropdownList[item.name] = [
            { id: 1, itemName: "Null Count", category: "Simple Statistics" },
            { id: 2, itemName: "Distinct Count", category: "Simple Statistics" },
            { id: 3, itemName: "Total Count", category: "Summary Statistics" },
            // {"id":8,"itemName":"Rule Detection Count","category": "Advanced Statistics"},
            { id: 9, itemName: "Enum Detection Top5 Count", category: "Advanced Statistics" }
            // {"id":10,"itemName":"Regular Expression Detection Count","category": "Advanced Statistics"}
          ];
        }
      }
    }
  }

  toggleSelection(row) {
    row.selected = !row.selected;
    var idx = this.selection.indexOf(row);
    // is currently selected
    if (idx > -1) {
      this.selection.splice(idx, 1);
      this.selectedAll = false;
      for (let key in this.selectedItems) {
        if (key === row.name) {
          delete this.selectedItems[key];
        }
      }
    } else {
      // is newly selected
      this.selection.push(row);
    }
    if (this.selection.length == 3) {
      this.selectedAll = true;
    } else {
      this.selectedAll = false;
    }
    this.setDropdownList();
  }

  toggleAll() {
    this.selectedAll = !this.selectedAll;
    this.selection = [];
    for (var i = 0; i < this.schemaCollection.length; i++) {
      this.schemaCollection[i].selected = this.selectedAll;
      if (this.selectedAll) {
        this.selection.push(this.schemaCollection[i]);
      }
    }
    this.setDropdownList();
  }

  transferRule(rule, col) {
    switch (rule) {
      case "Total Count":
        return "count(source.`" + col.name + "`) AS `" + col.name + "-count`";
      case "Distinct Count":
        return (
          "approx_count_distinct(source.`" +
          col.name +
          "`) AS `" +
          col.name +
          "-distcount`"
        );
      case "Null Count":
        return (
          "count(source.`" +
          col.name +
          "`) AS `" +
          col.name +
          "-nullcount" +
          "` WHERE source.`" +
          col.name +
          "` IS NULL"
        );
      // case 'Regular Expression Detection Count':
      //   return 'count(source.`'+col.name+'`) where source.`'+col.name+'` LIKE ';
      // case 'Rule Detection Count':
      //   return 'count(source.`'+col.name+'`) where source.`'+col.name+'` LIKE ';
      case "Maximum":
        return "max(source.`" + col.name + "`) AS `" + col.name + "-max`";
      case "Minimum":
        return "min(source.`" + col.name + "`) AS `" + col.name + "-min`";
      // case 'Median':
      //   return 'median(source.`'+col.name+'`) ';
      case "Average":
        return "avg(source.`" + col.name + "`) AS `" + col.name + "-average`";
      case "Enum Detection Top5 Count":
        return (
          "source.`" +
          col.name +
          "`,count(*) AS `" +
          "count` GROUP BY source.`" +
          col.name +
          "` ORDER BY `count` DESC LIMIT 5"
        );
    }
  }

  next(form) {
    if (this.formValidation(this.currentStep)) {
      this.currentStep++;
    } else {
      this.toasterService.pop(
        "error",
        "Error!",
        "Please select at least one attribute!"
      );
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
      var selectedlen = 0;
      for (let key in this.selectedItems) {
        selectedlen++;
        len = this.selectedItems[key].length;
        if (len == 0) {
          return false;
        }
      }
      return this.selection.length == selectedlen ? true : false;
    } else if (step == 3) {
      return true;
    } else if (step == 4) {
    }
    return false;
  };

  prev(form) {
    this.currentStep--;
  }
  goTo(i) {
    this.currentStep = i;
  }
  submit(form) {
    if (!form.valid) {
      this.toasterService.pop(
        "error",
        "Error!",
        "please complete the form in this step before proceeding"
      );
      return false;
    }
    this.newMeasure = {
      name: this.name,
      "measure.type": "griffin",
      "dq.type": "PROFILING",
      "rule.description": {
        details:this.noderule
      },
      "process.type": "BATCH",
      owner: this.owner,
      description: this.desc,
      // "group":this.finalgrp,
      "data.sources": [
        {
          name: "source",
          connectors: [
            {
              name: this.srcname,
              type: "HIVE",
              version: "1.2",
              "data.unit": this.size,
              "data.time.zone": this.timezone,
              config: {
                database: this.currentDB,
                "table.name": this.currentTable,
                where: this.where
              },
              predicates: [
                {
                  type: "file.exist",
                  config: {
                    "root.path": this.srclocation,
                    path: this.path
                  }
                }
              ]
            }
          ]
        }
      ],
      "evaluate.rule": {
        rules: [
          // {
          //   "dsl.type": "griffin-dsl",
          //   "dq.type": "profiling",
          //   "rule": "",
          //   "details": {}
          // }
        ]
      }
    };
    this.getGrouprule();
    if (this.size.indexOf("0") == 0) {
      delete this.newMeasure["data.sources"][0]["connectors"][0]["data.unit"];
    }
    if (!this.needpath || this.path == "") {
      delete this.newMeasure["data.sources"][0]["connectors"][0]["predicates"];
    }
    this.visible = true;
    setTimeout(() => (this.visibleAnimate = true), 100);
  }

  getRule(trans) {
    var rule = "";
    for (let i of trans) {
      rule = rule + i + ",";
    }
    rule = rule.substring(0, rule.lastIndexOf(","));
    this.pushRule(rule);
  }

  pushEnmRule(rule, grpname) {
    var self = this;
    self.newMeasure["evaluate.rule"].rules.push({
      "dsl.type": "griffin-dsl",
      "dq.type": "PROFILING",
      rule: rule,
      name: grpname,
      metric: {
        "collect.type": "array"
      }
    });
  }

  pushNullRule(rule, nullname) {
    var self = this;
    self.newMeasure["evaluate.rule"].rules.push({
      "dsl.type": "griffin-dsl",
      "dq.type": "PROFILING",
      rule: rule,
      name: nullname
    });
  }

  pushRule(rule) {
    var self = this;
    self.newMeasure["evaluate.rule"].rules.push({
      "dsl.type": "griffin-dsl",
      "dq.type": "PROFILING",
      rule: rule,
      name: "profiling"
    });
  }

  save() {
    var addModels = this.serviceService.config.uri.addModels;
    $("#save").attr("disabled", "true");
    this.http.post(addModels, this.newMeasure).subscribe(
      data => {
        this.createResult = data;
        this.hide();
        this.router.navigate(["/measures"]);
      },
      err => {
        let response = JSON.parse(err.error);
        if(response.code === '40901'){
          this.toasterService.pop("error", "Error!", "Measure name already exists!");
        } else {
          this.toasterService.pop("error", "Error!", "Error when creating measure");
        }
        console.log("Error when creating measure");
      }
    );
  }

  options: ITreeOptions = {
    displayField: "name",
    isExpandedField: "expanded",
    idField: "id",
    actionMapping: {
      mouse: {
        click: (tree, node, $event) => {
          if (node.hasChildren) {
            this.currentDB = node.data.name;
            this.currentDBstr = this.currentDB + ".";
            this.currentTable = "";
            this.schemaCollection = [];
            this.selectedAll = false;
            TREE_ACTIONS.TOGGLE_EXPANDED(tree, node, $event);
          } else if (node.data.cols) {
            this.currentTable = node.data.name;
            this.currentDB = node.data.parent;
            this.schemaCollection = node.data.cols;
            this.srcname = "source" + new Date().getTime();
            this.srclocation = node.data.location;
            this.selectedAll = false;
            this.selection = [];
            for (let row of this.schemaCollection) {
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

  nodeList: object[];
  nodeListTarget: object[];

  constructor(
    private elementRef: ElementRef,
    toasterService: ToasterService,
    private http: HttpClient,
    private router: Router,
    public serviceService: ServiceService
  ) {
    this.toasterService = toasterService;
    this.selection = [];
  }

  getGrouprule() {
    var selected = { name: "" };
    var value = "";
    var nullvalue = "";
    var nullname = "";
    var enmvalue = "";
    var grpname = "";
    for (let key in this.selectedItems) {
      selected.name = key;
      var info = "";
      var otherinfo = "";
      for (let i = 0; i < this.selectedItems[key].length; i++) {
        var originrule = this.selectedItems[key][i].itemName;
        info = info + originrule + ",";
        if (originrule == "Enum Detection Top5 Count") {
          enmvalue = this.transferRule(originrule, selected);
          grpname = selected.name + "-grp";
          this.transenumrule.push(enmvalue);
          this.pushEnmRule(enmvalue, grpname);
        } else if (originrule == "Null Count") {
          nullvalue = this.transferRule(originrule, selected);
          nullname = selected.name + "-nullct";
          this.transnullrule.push(nullvalue);
          this.pushNullRule(nullvalue, nullname);
        } else {
          otherinfo = otherinfo + originrule + ",";
          value = this.transferRule(originrule, selected);
          this.transrule.push(value);
        }
      }
      info = info.substring(0, info.lastIndexOf(","));
      otherinfo = otherinfo.substring(0, otherinfo.lastIndexOf(","));
      this.noderule.push({
        name: key,
        infos: info
      });
    }
    if (this.transrule.length != 0) {
      this.getRule(this.transrule);
    }
    this.ruledesc = JSON.stringify(this.noderule);
  }

  confirmAdd() {
    document.getElementById("rule").style.display = "none";
  }

  showRule() {
    document.getElementById("showrule").style.display = "";
    document.getElementById("notshowrule").style.display = "none";
  }

  back() {
    document.getElementById("showrule").style.display = "none";
    document.getElementById("notshowrule").style.display = "";
  }

  getData(evt) {
    this.config = evt;
    this.timezone = evt.timezone;
    this.where = evt.where;
    this.size = evt.num + evt.timetype;
    this.needpath = evt.needpath;
    this.path = evt.path;
  }

  ngOnInit() {
    var allDataassets = this.serviceService.config.uri.dataassetlist;
    this.http.get(allDataassets).subscribe(data => {
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
        for (let i = 0; i < this.data[db].length; i++) {
          let new_child = new node();
          new_child.name = this.data[db][i]["tableName"];
          new_node.children.push(new_child);
          new_child.isExpanded = false;
          new_child.location = this.data[db][i]["sd"]["location"];
          new_child.parent = db;
          new_child.cols = Array<Col>();
          for (let j = 0; j < this.data[db][i]["sd"]["cols"].length; j++) {
            let new_col = new Col(
              this.data[db][i]["sd"]["cols"][j].name,
              this.data[db][i]["sd"]["cols"][j].type,
              this.data[db][i]["sd"]["cols"][j].comment,
              false
            );
            new_child.cols.push(new_col);
          }
        }
        this.nodeList.push(new_node);
      }
      this.nodeListTarget = JSON.parse(JSON.stringify(this.nodeList));
    });
    this.dropdownSettings = {
      singleSelection: false,
      text: "Select Rule",
      enableCheckAll: false,
      enableSearchFilter: true,
      classes: "myclass",
      groupBy: "category"
    };
    this.size = "1day";
  }
  ngAfterViewChecked() {
    this.resizeWindow();
  }
}
