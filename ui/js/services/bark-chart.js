/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
define(['./module'], function (services) {
  services.factory('$barkChart', function(){

    return {
      getOptionPie: getOptionPie,
      getOptionBig: getOptionBig,
      getOptionSide: getOptionSide,
      getOptionThum: getOptionThum
    };
  });

  function getOptionPie(status) {
    var data = [];
    for (var key in status) {
        var item = {};
        item.name = key;
        item.value = status[key];
        if (key === 'health') {
            item.selected = true;
        }
        data.push(item);
    }

    var option = {
        title: {
            show: false
        },
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            formatter: "{b} <br> <b>{c}</b> ({d}%)"
        },
        series: [{
            type: 'pie',
            selectedMode: 'single',
            label:{
            normal: {
                position: 'inner',
                formatter: function(params) {
                    if (params.name === 'health') {
                    return params.percent+'%';
                    } else {
                    return '';
                    }
                }
            }
            },
            data: data
        }],
        color: ['#005732','#ff3333','#c23531']
    };

    return option;
  }

  function getOptionThum(metric) {
    var data = getMetricData(metric);
    var option = {
      title: {
        text:  metric[0]._source.name,
        left: 'center',
        textStyle: {
            fontWeight: 'normal',
            fontSize: 15
        }
      },
      backgroundColor: 'transparent',
      grid:{
        right: '7%',
        left: '5%',
        bottom: '5%',
        containLabel: true
      },
      tooltip : {
          trigger: 'axis',
          formatter : function(params) {
            return getTooltip(params);
          },
          position: function(point, params, dom) {
              return getTooltipPosition(point, params, dom);
          }
      },
      xAxis : {
              type : 'time',
              splitLine: {
                  show: false
              },
              splitNumber: 2
      },
      yAxis : {
              type : 'value',
              scale : true,
              name: formatter_yaxis_name(metric),
              axisLabel: {
                  formatter: formatter_value
              },
              splitNumber: 2
      }
    };
    option.series = getSeries(metric);
    return option;
  }

  function getTooltipPosition(point, params, dom) {
      return [point[0]/2, point[1]/2];
  }

  function formatter_value(value, index) {
      if (value < 1000) {
          return value;
      } else {
          return value/1000;
      }
  }

  function formatter_yaxis_name(metric) {
//      if (metric.hits.hits <= 100) {
          return 'accuracy (%)';
//      } else {
//          return 'dq (k)';
//      }
  }

  function getTooltip(params) {
    var result = '';
    if (params.length > 0) {
          result = new Date(getUTCTimeStamp(params[0].data[0])).toUTCString().replace('GMT', '')+
                      '<br /> Value : ' + params[0].data[1];
    }
    return result;
  }

  function getUTCTimeStamp(timestamp) {
    var TzOffset = new Date(timestamp).getTimezoneOffset()/60;
    return timestamp-TzOffset*60*60*1000;
  }

  function getOptionSide(metric) {
    var data = getMetricData(metric);
    var option = {
      title: {
        show: false
      },
      backgroundColor: 'transparent',
      grid:{
        right: '5%',
        left: '5%',
        bottom: '5%',
        top: 30,
        containLabel: true

      },
      tooltip : {
          trigger: 'axis',
          formatter : function(params) {
            return getTooltip(params);
          }
      },
      xAxis : {
              type : 'time',
              splitLine: {
                  show: false
              },
              splitNumber: 2
      },
      yAxis : {
              type : 'value',
              scale : true,
              name: formatter_yaxis_name(metric),
              axisLabel: {
                  formatter: formatter_value
              },
              splitNumber: 2
      }
    };
    option.series = getSeries(metric);
    return option;
  }

  function getSeries(metric) {
    var series = {};
    series = getSeriesCount(metric);
    return series;
  }

  function getOptionBig(metric) {
    var data = getMetricData(metric);
    var option = {
      title: {
        text:  metric[0]._source.name,
//        link: '/#/viewrule/' + metric.name,
//        target: 'self',
        left: 'center',
        textStyle: {
            fontSize: 25
        }
      },
      grid: {
        right: '2%',
        left: '2%',
        containLabel: true
      },
      dataZoom: [{
        type: 'inside',
        start: 0,
        throttle: 50
      },{
        show: true,
        start: 0
      }],
      tooltip : {
          trigger: 'axis',
          formatter : function(params) {
            return getTooltip(params);
          }
      },
      xAxis : {
              type : 'time',
              splitLine: {
                  show: false
              }
      },
      yAxis : {
              type : 'value',
              scale : true,
              name: formatter_yaxis_name(metric),
              axisLabel: {
                  formatter: formatter_value
              }
      },
      animation: true
    };
    option.series = getSeries(metric);
    return option;
  }

  function getMetricData(metric) {
    var data = [];
    var chartData = metric;
    for(var i = 0; i < chartData.length; i++){
        data.push([formatTimeStamp(chartData[i]._source.tmst), parseFloat((chartData[i]._source.matched/chartData[i]._source.total).toFixed(2))]);
    }

    data.sort(function(a, b){
      return a[0] - b[0];
    });

    return data;
  }

  function formatTimeStamp(timestamp) {
      var TzOffset = new Date(timestamp).getTimezoneOffset()/60-7;
      return timestamp+TzOffset*60*60*1000;
  }

  function getSeriesCount(metric) {
    var series = [];
    var data = getMetricData(metric);
    series.push({
          type: 'line',
          data: data,
          smooth:true,
          lineStyle: {
            normal: {
                color: '#d48265'
            }
          },
          itemStyle: {
              normal: {
                  color: '#d48265'
              }
          }
      });
      return series;
  }


});
