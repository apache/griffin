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
define(['./module'], function(controllers) {
    'use strict';
    controllers.controller('DownloadSampleCtrl', ['$scope', '$http', '$config', '$filter', '$timeout', '$compile', '$routeParams', '$barkChart', '$rootScope', function($scope, $http, $config, $filter, $timeout, $compile, $routeParams, $barkChart, $rootScope) {

        $scope.downloadUrl = $config.uri.metricdownload;

        $scope.$on('downloadSample', function(e, itemName) {
            $scope.selectedModelName = itemName;
            var sampleUrl = $config.uri.metricsample + '/' + itemName;
            $http.get(sampleUrl).success(function(data){
                $scope.sample = data;
                $('#download-sample').modal('show');
                $('#viewsample-content').css({
                    'max-height': window.innerHeight - $('.modal-content').offset().top - $('.modal-header').outerHeight() - $('.modal-footer').outerHeight() - 250
                });

            });
        });

    }
    ]);
});