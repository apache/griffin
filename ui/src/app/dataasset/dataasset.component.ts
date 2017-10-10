import { Component, OnInit } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import { Ng2SmartTableModule ,LocalDataSource} from 'ng2-smart-table';
import * as $ from 'jquery';
import {ServiceService} from '../service/service.service';

@Component({
  selector: 'app-dataasset',
  templateUrl: './dataasset.component.html',
  providers:[ServiceService],
  styleUrls: ['./dataasset.component.css']
})
export class DataassetComponent implements OnInit {
  public results = [];
  source:LocalDataSource;
  public visible = false;
  public visibleAnimate = false;
  sourceTable :string;
  targetTable :string;
  data:object;

  public hide(): void {
    this.visibleAnimate = false;
    setTimeout(() => this.visible = false, 300);
  }

  public onContainerClicked(event: MouseEvent): void {
    if ((<HTMLElement>event.target).classList.contains('modal')) {
      this.hide();
    }
  }
  constructor(private http:HttpClient,public servicecService:ServiceService) { }
  parseDate(time){
    time = new Date(time);
    var year = time.getFullYear();
    var month = time.getMonth() + 1;
    var day = time.getDate();
    var hour = time.getHours();
    if(hour<10)
      hour = '0' + hour;
    var minute = time.getMinutes();
    if(minute<10)
      minute = '0' + minute;
    var second = time.getSeconds();
    if(second<10)
      second = '0' + second;
    return  ( year +'/'+ month + '/'+ day + ' '+ hour + ':' + minute + ':' + second);
  }


  ngOnInit() {
    var allDataassets = this.servicecService.config.uri.dataassetlist;
    this.http.get(allDataassets).subscribe(data =>{
        for (let db in data) {
            for(let table of data[db]){           
            table.location = table.sd.location;
            this.results.push(table);
            }       
        }
        this.source = new LocalDataSource(this.results);
        this.source.load(this.results);
        $('.icon').hide();
    },err =>{
      
    });
  };
}
