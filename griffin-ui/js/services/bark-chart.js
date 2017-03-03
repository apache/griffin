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
        text:  metric.name,
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
            return getTooltip(params, metric.metricType);
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
      if (metric.dq <= 100) {
          return 'dq (%)';
      } else {
          return 'dq (k)';
      }
  }

  function getTooltip(params, metricType) {
    var result = '';
    if (params.length > 0) {

        if(metricType == 'Bollinger'){
          result = new Date(getUTCTimeStamp(params[0].data[0])).toUTCString().replace('GMT', '')+
                      '<br /> Value : ' + params[2].data[1] +
                      '<br /> Average : ' + params[3].data[1] +
                      '<br /> Bands : ' + params[0].data[1] + '--' + params[1].data[1];
        }else if(metricType == 'Trend'){
          result = new Date(getUTCTimeStamp(params[0].data[0])).toUTCString().replace('GMT', '')+
                      '<br /> Value : ' + params[0].data[1] +
                      '<br /> -7 days : ' + params[1].data[1];
        }else if(metricType == 'MAD'){
          result = new Date(getUTCTimeStamp(params[0].data[0])).toUTCString().replace('GMT', '')+
                      '<br /> Value : ' + params[2].data[1] +
                      '<br /> Bands : ' + params[0].data[1] + '--' + params[1].data[1];
        }else if(metricType == 'Count' || metricType == ''){
          result = new Date(getUTCTimeStamp(params[0].data[0])).toUTCString().replace('GMT', '')+
                      '<br /> Value : ' + params[0].data[1];
        }
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
            return getTooltip(params, metric.metricType);
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
    if(metric.metricType == 'Bollinger'){
      series = getSeriesBollinger(metric);
    }else if(metric.metricType == 'Trend'){
      series = getSeriesTrend(metric);
    }else if(metric.metricType == 'MAD'){
      series = getSeriesMAD(metric);
    }else if(metric.metricType == 'Count' || metric.metricType == ''){
      series = getSeriesCount(metric);
    }
    return series;
  }

  function getOptionBig(metric) {
    var data = getMetricData(metric);
    var option = {
      title: {
        text:  metric.name,
        link: '/#/viewrule/' + metric.name,
        target: 'self',
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
        start: 75,
        throttle: 50
      },{
        show: true,
        start: 75
      }],
      tooltip : {
          trigger: 'axis',
          formatter : function(params) {
            return getTooltip(params, metric.metricType);
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
    if (metric.metricType == 'MAD') {
        option.series = getMADBigSeries(option.series);
    } else if (metric.metricType == 'Bollinger') {
        option.series = getBollingerBigSeries(option.series);
    }
    return option;
  }

  function getBollingerBigSeries(series) {
      var dataLow = series[0].data;
      var data = series[2].data;
      var result = [];
      for (var i = 0; i < data.length; i++) {
          if (data[i][1] < dataLow[i][1]) {
              var item = {};
              item.coord = data[i];
              var diff = Number(dataLow[i][1])-Number(data[i][1]);
              item.label = {
                  normal: {
                      formatter: 'low '+diff
                  }
              };
              item.itemStyle = {
                  normal: {
                      color: '#c23531'
                  }
              };
              result.push(item);
          }
      }
      series[2].markPoint = {};
      series[2].markPoint.data = result;
      console.log(series);
      return series;
  }

  function getMADBigSeries(series) {
      var dataLow = series[0].data;
      var data = series[2].data;
      var result = [];
      for (var i = 0; i < data.length; i++) {
          if (data[i][1] < dataLow[i][1]) {
              var item = {};
              item.coord = data[i];
              var diff = Number(dataLow[i][1])-Number(data[i][1]);
              item.label = {
                  normal: {
                      formatter: Math.round(diff/1000) + 'K below lower band'
                  }
              };
              item.itemStyle = {
                  normal: {
                      color: '#c23531'
                  }
              };
              result.push(item);
          }
      }
      series[2].markPoint = {};
      series[2].markPoint.data = result;
      console.log(series);
      return series;
  }

  function getMetricData(metric) {
    var data = [];
    var chartData = metric.details;
    for(var i = 0; i < chartData.length; i++){
        data.push([formatTimeStamp(chartData[i].timestamp), parseFloat(chartData[i].value.toFixed(2))]);
    }

    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesMADLow(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].mad.lower)]);
    }
    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesMADUp(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].mad.upper-chartData[i].mad.lower)]);
    }
    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesBollingerLow(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].bolling.lower)]);
    }
    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesBollingerUp(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].bolling.upper-chartData[i].bolling.lower)]);
    }
    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesBollingerMean(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].bolling.mean)]);
    }
    data.sort(function(a, b){
      return a[0] - b[0];
    });
    return data;
  }

  function getSeriesTrendComparision(metric) {
    var data = [];
    var chartData = metric.details;
    for (var i = 0; i < chartData.length; i++) {
      data.push([formatTimeStamp(chartData[i].timestamp), Number(chartData[i].comparisionValue)]);
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

  function getSeriesTrend(metric) {
    var series = [];
    var data = getMetricData(metric);
    var dataComparision = getSeriesTrendComparision(metric);
    series.push({
          type: 'line',
          smooth:true,
          data: data,
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
      series.push({
          type: 'line',
          smooth:true,
          data: dataComparision,
          lineStyle: {
            normal: {
                color: '#f15c80',
                type: 'dashed'
            }
          },
          itemStyle: {
              normal: {
                  color: '#f15c80'
              }
          }
      });
      return series;
  }

  function getSeriesBollinger(metric) {
    var series = [];
      var dataLow = getSeriesBollingerLow(metric);
      var dataUp = getSeriesBollingerUp(metric);
      var dataMean = getSeriesBollingerMean(metric);
      var data = getMetricData(metric);
      series.push({
          name: 'L',
          type: 'line',
          smooth:true,
          data: dataLow,
          lineStyle: {
              normal: {
                  opacity: 0
              }
          },
          stack: 'MAD-area',
          symbol: 'none'
      });
      series.push({
          name: 'U',
          type: 'line',
          smooth:true,
          data: dataUp,
          lineStyle: {
              normal: {
                  opacity: 0
              }
          },
          areaStyle: {
              normal: {
                  color: '#eee',
                  opacity: 0.2
              }
          },
          stack: 'MAD-area',
          symbol: 'none'
      });
      series.push({
          type: 'line',
          smooth:true,
          data: data,
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
      series.push({
          type: 'line',
          smooth:true,
          data: dataMean,
          lineStyle: {
            normal: {
                color: '#f15c80',
                type: 'dashed'
            }
          },
          itemStyle: {
              normal: {
                  color: '#f15c80'
              }
          }
      });
      return series;
  }

  function getSeriesMAD(metric) {
      var series = [];
      var dataLow = getSeriesMADLow(metric);
      var dataUp = getSeriesMADUp(metric);
      var data = getMetricData(metric);
      series.push({
          name: 'L',
          type: 'line',
          smooth:true,
          data: dataLow,
          lineStyle: {
              normal: {
                  opacity: 0
              }
          },
          stack: 'MAD-area',
          symbol: 'none'
      });
      series.push({
          name: 'U',
          type: 'line',
          smooth:true,
          data: dataUp,
          lineStyle: {
              normal: {
                  opacity: 0
              }
          },
          areaStyle: {
              normal: {
                  color: '#eee',
                  opacity: 0.2
              }
          },
          stack: 'MAD-area',
          symbol: 'none'
      });
      series.push({
          type: 'line',
          smooth:true,
          data: data,
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
