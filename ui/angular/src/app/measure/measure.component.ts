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
import { Component, OnInit} from '@angular/core';
import { HttpClient} from '@angular/common/http';
import { Ng2SmartTableModule ,LocalDataSource} from 'ng2-smart-table';
import {DataTableModule} from "angular2-datatable";
import { Router} from "@angular/router";
import { FormControl } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import {ServiceService} from '../service/service.service';

import { BrowserAnimationsModule} from '@angular/platform-browser/animations';
import { ToasterModule, ToasterService} from 'angular2-toaster';
import * as $ from 'jquery';

@Component({
  selector: 'app-measure',
  templateUrl: './measure.component.html',
  providers:[ServiceService],
  styleUrls: ['./measure.component.css']
})
export class MeasureComponent implements OnInit {
  //results:object[];
  results:any;
  source:LocalDataSource;
  public visible = false;
  public visibleAnimate = false;
  deletedRow : any;
  // sourceTable :string;
  // targetTable :string;
  deleteId : number;
  deleteIndex:number;
  // measureData= [{"id":22,"name":"i","description":"i","organization":"waq","type":"accuracy","source":{"id":43,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":44,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":22,"sampleRatio":0,"rules":"$source['id'] == $target['id']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":23,"name":"y","description":"y","organization":"waq","type":"accuracy","source":{"id":45,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":46,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":23,"sampleRatio":0,"rules":"$source['age'] > $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":24,"name":"ggg","description":"g","organization":"waq","type":"accuracy","source":{"id":47,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":48,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":24,"sampleRatio":0,"rules":"$source['id'] !== $target['id']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":25,"name":"t","description":"t","organization":"qaq","type":"accuracy","source":{"id":49,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"target":{"id":50,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"evaluateRule":{"id":25,"sampleRatio":0,"rules":"$source['name'] >= $target['name']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":26,"name":"yyy","description":"yyy","organization":"yyy","type":"accuracy","source":{"id":51,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":52,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":26,"sampleRatio":0,"rules":"$source['id'] !== $target['name']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":27,"name":"dd","description":"dd","organization":"waq","type":"accuracy","source":{"id":53,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"target":{"id":54,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"evaluateRule":{"id":27,"sampleRatio":0,"rules":"$source['name'] !== $target['id']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":28,"name":"yyyyyyyyy","description":"y","organization":"waq","type":"accuracy","source":{"id":55,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":56,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":28,"sampleRatio":0,"rules":"$source['id'] !== $target['id']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":29,"name":"this","description":null,"organization":"hadoop","type":"accuracy","source":{"id":57,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":58,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":29,"sampleRatio":0,"rules":"$source['id'] == $target['id'] AND $source['name'] !== $target['name'] AND $source['age'] > $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":30,"name":"new","description":null,"organization":"griffin","type":"accuracy","source":{"id":59,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":60,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":30,"sampleRatio":0,"rules":"$source['id'] !== $target['id'] AND $source['name'] == $target['name'] AND $source['age'] > $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":31,"name":"www","description":"wn","organization":"waq","type":"accuracy","source":{"id":61,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":62,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":31,"sampleRatio":0,"rules":"$source['id'] > $target['id'] AND $source['name'] == $target['name'] AND $source['age'] !== $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":32,"name":"testtest","description":null,"organization":"waq","type":"accuracy","source":{"id":63,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"target":{"id":64,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"evaluateRule":{"id":32,"sampleRatio":0,"rules":"$source['id'] !== $target['id'] AND $source['name'] > $target['name'] AND $source['age'] == $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":33,"name":"search_hourly","description":"search","organization":"waq","type":"accuracy","source":{"id":65,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":66,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":33,"sampleRatio":0,"rules":"$source['id'] !== $target['id'] AND $source['name'] == $target['name'] AND $source['age'] >= $target['age']"},"owner":"test","deleted":false,"process.type":"batch"},{"id":34,"name":"testt","description":null,"organization":"waq","type":"accuracy","source":{"id":67,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext"}},"target":{"id":68,"type":"HIVE","version":"1.2","config":{"database":"default","table.name":"ext1"}},"evaluateRule":{"id":34,"sampleRatio":0,"rules":"$source['id'] !== $target['id'] AND $source['name'] == $target['name'] AND $source['age'] > $target['age']"},"owner":"test","deleted":false,"process.type":"batch"}];

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => this.visible = false, 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains('modal')) {
      this.hide();
    }
  }
 
  constructor(private http:HttpClient,private router:Router,public serviceService:ServiceService) { 
  };

  remove(row){
    this.visible = true;
    setTimeout(() => this.visibleAnimate = true, 100);
    this.deleteId = row.id;
    this.deleteIndex = this.results.indexOf(row);
    this.deletedRow = row;
    // this.sourceTable = this.deletedRow.source.config["table.name"];
    // this.targetTable = this.deletedRow.target.config["table.name"];
  }

  confirmDelete(){
    var deleteModel = this.serviceService.config.uri.deleteModel;
    let deleteUrl = deleteModel + '/' + this.deleteId;
    this.http.delete(deleteUrl).subscribe(data => {
      let deleteResult:any = data;
      console.log(deleteResult.code);
      if(deleteResult.code==202){
        var self = this;
        setTimeout(function () {
          self.results.splice(self.deleteIndex,1);
          // self.source.load(self.results);
          self.hide();
        },200);
      }
    });
  };

  ngOnInit():void {
    var allModels = this.serviceService.config.uri.allModels;
  	this.http.get(allModels).subscribe(data =>{
        for(let measure in data){
          data[measure].trueName = data[measure].name;
          if(data[measure].evaluateRule.rules[0]){
            data[measure].type = data[measure].evaluateRule.rules[0]["dq.type"];
          }else{
            data[measure].type = '';
          }
        }
  		  this.results = Object.keys(data).map(function(index){
          let measure = data[index];
          return measure;
        });
  	});
  // };
// }
   // this.results = this.measureData;
  }
}
