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
define(['./module'], function (directives) {
    'use strict';
    directives.directive( 'bigChart', ['$compile', '$timeout', function( $compile, $timeout ) {
      return {
        restrict: 'AE',
        templateUrl: '/pages/template/bigchart.html',
        // compile: function(element, attrs){
        //   return {
        //     pre: function(scope, element, attrs){
        //       // $('#bigChartShow').remove();
        //       console.log('pre');
        //
        //     },
        //     post: function(scope, element, attrs){
        //       console.log('post');
        //     }
        //   };
        // },
        // preLink: function( scope, element, attrs ){
        //   $('#bigChartShow').remove();
        // },
        // postLink: function( scope, element, attrs ){
        //   alert('hello');
        // // },
        link: function( scope, element, attrs ) {
          console.log(scope);
          var getWidth = function(){
            return window.innerWidth;
          }
          var getHeight = function(){
            return window.innerHeight;
          }

        //  $('#bigChartShow').remove();
          // if($('big-chart')){
          //   if($('big-chart').length > 1){
          //     $($('big-chart')[0]).remove();
          //   }
          // }

          $timeout(function(){
            $('#bigChartShow').css({height:getHeight(),
                                    width: getWidth()
                                  });

            $(window).resize(function(){
                $('#bigChartShow').css({height:getHeight(),
                            width: getWidth()
                          });
            });
          });

          // scope.$watch('chartConfig', function(newValue){
          //   resizeChart(newValue);
          // });

          // scope.$on('$routeChangeStart', function(){
          //   $('#mainWindow').show();
          //   $('#bigChartContainer').hide();
          // });

          // function resizeChart(config){
          //   $timeout(function(){
          //     config.options.chart.width = getWidth();
          //     config.options.chart.height = getHeight();
          //   });
          // }

          scope.closeBigChart = function(){
            console.log('close big chart!');
            $('#bigChartContainer').hide();
            $('#mainWindow').show();

          }
        }
      };
    }]);
});
