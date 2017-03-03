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
define(['./module'], function (controllers) {
    'use strict';
    controllers.controller('HealthCtrl', ['$scope', '$http', '$config', '$location','$timeout', '$route', '$barkChart', '$rootScope', function ($scope, $http, $config, $location, $timeout, $route, $barkChart, $rootScope) {
      console.log('health controller');
      // var url="/js/controllers/heatmap.json";

        var echarts = require('echarts');
        var formatUtil = echarts.format;

        pageInit();

        function pageInit() {
            $scope.$emit('initReq');

            var url = $config.uri.heatmap;
            $http.get(url).success(function(res) {
                renderTreeMap(res);
            });
        }

        function renderTreeMap(res) {


                function parseData(data) {
                    var sysId = 0;
                    var metricId = 0;
                    var result = [];
                    angular.forEach(res,function(sys,key){
                        console.log(sys);

                        var item = {};
                        item.id = 'id_'+sysId;
                        item.name = sys.name;

                        if (sys.metrics != undefined) {
                            item.children = [];
                            angular.forEach(sys.metrics,function(metric,key){
                                var itemChild = {
                                    id: 'id_' + sysId + '_' + metricId,
                                    name: metric.name,// + '(' + metric.dq + '%)',
                                    // value: metric.dq,
                                    value: 1,
                                    dq: metric.dq,
                                    sysName: sys.name,
                                    itemStyle: {
                                        normal: {
                                            color: '#4c8c6f'
                                        }
                                    }
                                };
                                if (metric.dqfail == 1) {
                                    itemChild.itemStyle.normal.color = '#ae5732';
                                } else {
                                    itemChild.itemStyle.normal.color = '#005732';
                                }
                                item.children.push(itemChild);
                                metricId++;
                            });
                        }

                        result.push(item);

                        sysId ++;
                    });
                    return result;
                }

                var data = parseData(res);
                console.log(data);

                function getLevelOption() {
                    return [
                        {
                            itemStyle: {
                                normal: {
                                    borderWidth: 0,
                                    gapWidth: 6,
                                    borderColor: '#000'
                                }
                            }
                        },

                        {
                            itemStyle: {
                                normal: {
                                    gapWidth: 1,
                                    borderColor: '#fff'
                                }
                            }
                        }
                    ];
                }

                var option = {

                    title: {
                        text: 'Data Quality Metrics Heatmap',
                        left: 'center'
                    },

                    backgroundColor: 'transparent',

                    tooltip: {
                        formatter: function(info) {
                            var dqFormat = info.data.dq>100?'':'%';
                            return [
                                '<span style="font-size:1.8em;">' + formatUtil.encodeHTML(info.data.sysName) + ' &gt; </span>',
                                '<span style="font-size:1.5em;">' + formatUtil.encodeHTML(info.data.name)+'</span><br>',
                                '<span style="font-size:1.5em;">dq : ' + info.data.dq.toFixed(2) + dqFormat + '</span>'
                            ].join('');
                        }
                    },

                    series: [
                        {
                            name:'System',
                            type:'treemap',
                            itemStyle: {
                                normal: {
                                    borderColor: '#fff'
                                }
                            },
                            levels: getLevelOption(),
                            breadcrumb: {
                                show: false
                            },
                            roam: false,
                            nodeClick: false,
                            data: data,
                            // leafDepth: 1,
                            width: '95%',
                            bottom : 0
                        }
                    ]
                };

                resizeTreeMap();
                $scope.myChart = echarts.init(document.getElementById('chart1'), 'dark');
                $scope.myChart.setOption(option);

                $scope.myChart.on('click', function(param) {
                    // if (param.data.sysName) {
                    //     $location.path('/metrics/' + param.data.sysName);
                    //     $scope.$apply();
                    //     return false;
                    // }
                    // param.event.event.preventDefault();
                    if (param.data.name) {

                        showBig(param.data.name);
                        // return false;
                    }
                });

        }

        var showBig = function(metricName){
          var metricDetailUrl = $config.uri.metricdetail + '/' + metricName;
          $http.get(metricDetailUrl).success(function (data){
            $rootScope.showBigChart($barkChart.getOptionBig(data));
          });
        }

        $scope.$on('resizeHandler', function(e) {
            if($route.current.$$route.controller == 'HealthCtrl'){
                console.log('health resize');
                resizeTreeMap();
                $scope.myChart.resize();
            }
        });

        function resizeTreeMap() {
            $('#chart1').height( $('#mainWindow').height() - $('.bs-component').outerHeight() );
        }

    }]);
});
