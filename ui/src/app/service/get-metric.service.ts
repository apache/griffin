import { Injectable } from '@angular/core';
import {ServiceService} from './service.service';
import {HttpClient} from '@angular/common/http';



@Injectable()
export class GetMetricService {

  constructor(private http:HttpClient,public servicecService:ServiceService) { }

  orgs = [];
  finalData = [];
  originalOrgs = [];
  status:{
  	'health':number,
  	'invalid':number
  };
  chartOption = new Map();
  // var formatUtil = echarts.format;
  metricData = [];
  originalData :any;
  metricName = [];
  metricNameUnique = [];
  myData = [];
//   allData = {
//   "hits" : {
//     "hits" : [
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493962623461,
//           "total" : 8043288,
//           "matched" : 8034775
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493973423461,
//           "total" : 9479698,
//           "matched" : 9476094
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493987823461,
//           "total" : 9194117,
//           "matched" : 9164237
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1493995023461,
//           "total" : 9429018,
//           "matched" : 9375324
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493959023461,
//           "total" : 1086389,
//           "matched" : 1083336
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493973423461,
//           "total" : 1090650,
//           "matched" : 1090445
//         }
//       },
//       {
//         "_source" : {
//           "name" : "xixi",
//           "tmst" : 1494009423461,
//           "total" : 8029660,
//           "matched" : 7979653
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493980623461,
//           "total" : 1088940,
//           "matched" : 1079003
//         }
//       },
//       {
//         "_source" : {
//           "name" : "haha",
//           "tmst" : 1493995023461,
//           "total" : 1048833,
//           "matched" : 1047890
//         }
//       },
//       {
//         "_source" : {
//           "name" : "search_hourly",
//           "tmst" : 1493948223461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948224461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "search_hourly",
//           "tmst" : 1493948225461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948226461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948223461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "hh",
//           "tmst" : 1493948224461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948225461,
//           "total" : 100,
//           "matched" : 99
//         }
//       },
//       {
//         "_source" : {
//           "name" : "buy_hourly",
//           "tmst" : 1493948226461,
//           "total" : 100,
//           "matched" : 99
//         }
//       }
//     ]
//   }
// };
     // let orgWithMeasure = {"waq":["waq","search_hourly"],"xi":["xixi","haha"],"hadoop":["viewitem_hourly","buy_hourly","hh"]};

  renderData(){
  	var url_organization = this.servicecService.config.uri.organization;
    this.http.get(url_organization).subscribe(data => {
      let orgWithMeasure = data;
      var orgNode = null;
      for(let orgName in orgWithMeasure){
        orgNode = new Object();
        orgNode.name = orgName;
        orgNode.measureMap = orgWithMeasure[orgName];
        this.orgs.push(orgNode);
      }
      this.originalOrgs = this.orgs;
      let url_dashboard = this.servicecService.config.uri.dashboard;
      this.http.post(url_dashboard, {"query": {"match_all":{}},  "sort": [{"tmst": {"order": "asc"}}],"size":1000}).subscribe(data => {
            this.originalData = JSON.parse(JSON.stringify(data));
            this.myData = JSON.parse(JSON.stringify(this.originalData.hits.hits));
            this.metricName = [];
            for(var i = 0;i<this.myData.length;i++){
                this.metricName.push(this.myData[i]._source.name);
            }
            this.metricNameUnique = [];
            for(let name of this.metricName){
                if(this.metricNameUnique.indexOf(name) === -1){
                    this.metricData[this.metricNameUnique.length] = new Array();
                    this.metricNameUnique.push(name);
                }
            };
            for(var i = 0;i<this.myData.length;i++){
                for(var j = 0 ;j<this.metricNameUnique.length;j++){
                    if(this.myData[i]._source.name==this.metricNameUnique[j]){
                        this.metricData[j].push(this.myData[i]);
                    }
                }
            }
            for(let sys of this.originalOrgs){
                var node = null;
                node = new Object();
                node.name = sys.name;
                node.dq = 0;
                node.metrics = new Array();
                for (let metric of this.metricData){
                    if(sys.measureMap.indexOf(metric[metric.length-1]._source.name)!= -1){
                        var metricNode = {
                            'name':'',
                            'timestamp':'',
                            'dq':0,
                            'details':[]
                        }
                        metricNode.name = metric[metric.length-1]._source.name;
                        metricNode.timestamp = metric[metric.length-1]._source.tmst;
                        metricNode.dq = metric[metric.length-1]._source.matched/metric[metric.length-1]._source.total*100;
                        metricNode.details = metric;
                        node.metrics.push(metricNode);
                    }
                }
                this.finalData.push(node);
  			    }
            console.log(this.finalData);
            return JSON.parse(JSON.stringify(this.finalData));
      });
    });
	};
}
