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
import { Component, OnInit, AfterViewChecked, ViewChildren } from "@angular/core";
import { FormControl } from "@angular/forms";
import { FormsModule } from "@angular/forms";
import { MaxLengthValidator } from "@angular/forms";
import { NgControlStatus, Validators } from "@angular/forms";
import { PatternValidator } from "@angular/forms";
import { MatDatepickerModule } from "@angular/material";
import { ServiceService } from "../../service/service.service";
import { AngularMultiSelectModule } from "angular2-multiselect-dropdown/angular2-multiselect-dropdown";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { ToasterModule, ToasterService, ToasterConfig } from "angular2-toaster";
import * as $ from "jquery";
import { HttpClient, HttpParams } from "@angular/common/http";
import { Router } from "@angular/router";
import { NouisliderModule } from "ng2-nouislider";
import {CommonUtil} from "../../common/common.util";

@Component({
  selector: "app-create-dataasset",
  templateUrl: "./create-dataasset.component.html",
  providers: [ServiceService],
  styleUrls: ["./create-dataasset.component.css"]
})
export class CreateDataassetComponent implements OnInit, AfterViewChecked {
  constructor(
    toasterService: ToasterService,
    private http: HttpClient,
    private router: Router,
    public serviceService: ServiceService
  ) {
    this.toasterService = toasterService;
  }

  @ViewChildren("sliderRef") sliderRefs;

  dataAsset: CommonUtil.DataAsset;
  iRow: number = 0; 

  isKafka=true;
  selectedRow : Number;

  keysKafaka = [
    "AssetName", 
    "BootstrapServers", 
    "GroupId", 
    "AutoOffsetReset",
    "AutoCommitEnable",
    "Topics", 
    "KeyType",
    "ValueType",
    "PreProcs",
    "DslType",
    "Name",
    "Rule",
    "Details",
    "DfName",
    "ColName",
    "Updatable"
  ];

  labelsKafaka = {
    BootstrapServers: "bootstrap.servers",
    GroupId: "group.id",
    AutoOffsetReset: "auto.offset.reset",
    AutoCommitEnable: "auto.commit.enable",
    Topics: "topics",
    KeyType: "key.type",
    ValueType: "value.type",
    PreProcs: "pre.proc",
    DslType: "dsl.type",
    Name: "name",
    Rule: "rule",
    Details: "details",
    DfName: "df.name",
    ColName: "col.name",
    Updatable: "updatable"
  };

  AutoOffsetResetOptions=["largest", "earliest"];
  DslTypeOptions = ["df-opr", "spark-sql"];
  PreProcRuleOptions = ["from_json", "spark_sql"];
  BooleanOptions=["true", "false"];
  keyType="java.lang.String";
  valueType="java.lang.String";

  maskOpen = false;
  keyupLabelOn = false;
  keydownLabelOn = false;
  createResult = "";
  measure: string;
  measureid: any;

  

  private toasterService: ToasterService;
  public visible = false;
  public visibleAnimate = false;

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => (this.visible = false), 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains("modal")) {
      this.hide();
    }
  }

  close() {
    this.maskOpen = false;
  }

  prev() {
    history.back();
  }

  setClickedRowInPreProc(index){
    this.selectedRow = index;
  }

  addRow() {
    this.dataAsset.Kafka.PreProcs.push(CommonUtil.addPreProc(this.iRow ));
    console.log("this.iRow =" + this.iRow);
    this.rowValidateForm(this.iRow++, 'add');
  }
  removeRow(index) {
    let controlIndex = this.dataAsset.Kafka.PreProcs[index].Id;
    console.log(index, controlIndex, "this.iRow =" + this.iRow);
    this.dataAsset.Kafka.PreProcs.splice(index, 1);
    this.rowValidateForm(controlIndex, 'remove');
  }

  rowValidateForm(i: number, scenario?: string) {
    let action = "addControl";
    if (scenario == 'remove') {
      action = "removeControl";
    }
  }

  submit(form) {
    //TODO code changes here when service is ready
    if (!form.valid) {
      this.toasterService.pop("error", "Error!", "Please complete the form!");
      return false;
    }   
    
    this.visible = true;
    setTimeout(() => (this.visibleAnimate = true), 100);
  }

  save() {
    //TODO code change here when service is ready
    // var addDataAsset = this.serviceService.config.uri.addDataAsset;
    // this.http.post(addDataAsset, this.dataAsset).subscribe(
    //   data => {
    //     this.createResult = data["results"];
    //     this.hide();
    //     this.router.navigate(["/dataassets"]);
    //   },
    //   err => {
    //     let response = JSON.parse(err.error);
    //     if(response.code === '40004'){
    //       this.toasterService.pop("error", "Error!", "Data asset name already exists!");
    //     } else {
    //       this.toasterService.pop("error", "Error!", "Error when creating data asset");
    //     }
    //     console.log("Error when creating data asset");
    //   }
    // );
  }

  onResize(event) {
    this.resizeWindow();
  }

  resizeWindow() {
    var stepSelection = ".formStep";
    $(stepSelection).css({
      height:
        window.innerHeight -
        $(stepSelection).offset().top -
        $("#footerwrap").outerHeight()
    });
    $("fieldset").height(
      $(stepSelection).height() -
        $(stepSelection + ">.stepDesc").height() -
        $(".btn-container").height() -
        200
    );
    $(".y-scrollable").css({
      height: $("fieldset").height()
    });
    $("#data-asset-pie").css({
      height: $("#data-asset-pie")
        .parent()
        .width(),
      width: $("#data-asset-pie")
        .parent()
        .width()
    });
  }

  setHeight() {
    $("#md-datepicker-0").height(250);
  }

  typeChange(assetType){
    console.log(assetType);
    //reset all other fields except assetType
    this.isKafka = !this.isKafka;
    this.dataAsset= CommonUtil.initDataAsset(); 
    this.dataAsset.AssetType = assetType;

  }


  onChange(measure) {
    //TODO code change when service is ready
    if(measure.toLowerCase()=="hdfs"){
      this.isKafka=false;
    }
    // this.dropdownList = [];
    // for (let index in this.Measures) {
    //   var map = this.Measures[index];
    //   if (measure == map.name) {
    //     var source = map["data.sources"];
    //     for (let i = 0; i < source.length; i++) {
    //       var details = source[i].connectors;
    //       for (let j = 0; j < details.length; j++) {
    //         if (details[j]["data.unit"] != undefined) {
    //           var table =
    //             details[j].config.database +
    //             "." +
    //             details[j].config["table.name"];
    //           var size = details[j]["data.unit"];
    //           var connectorname = details[j]["name"];
    //           var detail = {
    //             id: i + 1,
    //             name: table,
    //             size: size,
    //             connectorname: connectorname
    //           };
    //           this.dropdownList.push(detail);
    //         }
    //       }
    //     }
    //   }
    // }
    // for (let i = 0; i < this.dropdownList.length; i++) {
    //   this.someKeyboard[i] = [-1, 0];
    //   this.someKeyboardConfig[i] = JSON.parse(JSON.stringify(this.config));
    //   if (this.sliderRefs._results[i]) {
    //     this.sliderRefs._results[i].slider.updateOptions({
    //       range: {
    //         min: -10,
    //         max: 0
    //       }
    //     });
    //   }
    // }
  }

  ngOnInit() {   
    this.dataAsset =  CommonUtil.initDataAsset(); 
  }

  ngAfterViewChecked() {
    this.resizeWindow();
  }
}

