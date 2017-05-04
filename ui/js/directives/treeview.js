/*
  The MIT License (MIT)

  Copyright (c) 2013 Steve

  Permission is hereby granted, free of charge, to any person obtaining a copy of
  this software and associated documentation files (the "Software"), to deal in
  the Software without restriction, including without limitation the rights to
  use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
  the Software, and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
  FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
  COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
  IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/*
  Code copy from https://github.com/eu81273/angular.treeview
*/
define(['./module'], function (directives) {
    'use strict';
    directives.directive( 'treeModel', ['$compile', function( $compile ) {
      return {
        restrict: 'A',
        link: function ( scope, element, attrs ) {
          //tree id
          // var treeId = attrs.treeId;

          //tree model
          var treeModel = attrs.treeModel;

          //node id
          var nodeId = attrs.nodeId || 'id';

          //node label
          var nodeLabel = attrs.nodeLabel || 'label';

          //children
          var nodeChildren = attrs.nodeChildren || 'children';

          //select label function name
          var labelSelect = attrs.labelSelect || 'selectNodeLabel';

          var l1Icon = attrs.l1Icon || 'fa fa-database',
              l2Icon = attrs.l2Icon || 'fa fa-table';

          //tree template
          var template =
            '<ul>' +
              '<li ng-repeat="node in ' + treeModel + '">' +
                '<i class="collapsed fa fa-caret-right" ng-show="node.' + nodeChildren + '.length && node.collapsed" ng-click="selectNodeHead(node)"></i>' +
                '<i class="expanded fa fa-caret-down" ng-show="node.' + nodeChildren + '.length && !node.collapsed" ng-click="selectNodeHead(node)"></i>' +
                '<i class="' + l1Icon + '" ng-show="node.l1"></i>'  +
                '<i class="' + l2Icon + '" ng-show="node.l2"></i>&nbsp;'  +

                //'<span ng-class="node.selected" ng-click="selectNodeLabel(node)">{{node.' + nodeLabel + '}}</span>' +
                '<span ng-class="node.selected" ng-click="' + labelSelect + '(node)">{{node.' + nodeLabel + '}}</span>' +
                // '<div ng-hide="node.collapsed" tree-model="node.' + nodeChildren + '" node-id=' + nodeId + ' node-label=' + nodeLabel + ' node-children=' + nodeChildren + '></div>' +
                '<div ng-hide="node.collapsed" tree-model="node.' + nodeChildren + '" node-id=' + nodeId + ' node-label=' + nodeLabel + ' node-children=' + nodeChildren + ' label-select=' + labelSelect +'></div>' +
              '</li>' +
            '</ul>';


          //check tree id, tree model
          // if( treeId && treeModel ) {

            //root node
            if( attrs.angularTreeview ) {



              //if node head clicks,
              scope.selectNodeHead = scope.selectNodeHead || function( selectedNode ){

                //Collapse or Expand
                selectedNode.collapsed = !selectedNode.collapsed;
              };

              //if node label clicks,
              scope[labelSelect] = scope[labelSelect] || function( selectedNode ){

                //remove highlight from previous node
                if( scope.currentNode && scope.currentNode.selected ) {
                  scope.currentNode.selected = undefined;
                }

                if(selectedNode.children && selectedNode.children.length > 0){
                  scope.selectNodeHead(selectedNode);
                }else{
                  //set highlight to selected node
                  selectedNode.selected = 'selected';
                }

                //set currentNode
                scope.currentNode = selectedNode;
              };
            }



            //Rendering template.
            element.html('').append( $compile( template )( scope ) );
          // }
        }
      };
    }]);

    directives.directive( 'treeModelCheck', ['$compile', function( $compile ) {
      return {
        restrict: 'A',
        link: function ( scope, element, attrs ) {
          //tree id
          // var treeId = attrs.treeId;

          //tree model
          var treeModel = attrs.treeModelCheck;

          //node id
          var nodeId = attrs.nodeId || 'id';

          //node label
          var nodeLabel = attrs.nodeLabel || 'label';

          //children
          var nodeChildren = attrs.nodeChildren || 'children';

          //select label function name
          var labelSelect = attrs.labelSelect || 'selectNodeLabel';

          var l1Icon = attrs.l1Icon || 'fa fa-database',
              l2Icon = attrs.l2Icon || 'fa fa-table';

          //tree template
          var template =
            '<ul>' +
              '<li ng-repeat="node in ' + treeModel + '">' +
                '<i class="collapsed fa fa-caret-right" ng-show="node.' + nodeChildren + '.length && node.collapsed" ng-click="selectNodeHead(node)"></i>' +
                '<i class="expanded fa fa-caret-down" ng-show="node.' + nodeChildren + '.length && !node.collapsed" ng-click="selectNodeHead(node)"></i>' +
                '<span class="normal" ng-show="node.l3"></span>' +
                '<input type="checkbox" ng-checked="isChecked(node)" ng-click="changeCB(node, $event)" ng-if="node.l2 || node.l3" value="{{node.fullname}}"/>&nbsp;' +
                '<i class="' + l1Icon + '" ng-show="node.l1"></i>'  +
                '<i class="' + l2Icon + '" ng-show="node.l2"></i>&nbsp;'  +

                //'<span ng-class="node.selected" ng-click="selectNodeLabel(node)">{{node.' + nodeLabel + '}}</span>' +
                '<span ng-class="node.selected">{{node.' + nodeLabel + '}}</span>' +
                // '<div ng-hide="node.collapsed" tree-model="node.' + nodeChildren + '" node-id=' + nodeId + ' node-label=' + nodeLabel + ' node-children=' + nodeChildren + '></div>' +
                '<div ng-hide="node.collapsed" tree-model-check="node.' + nodeChildren + '" node-id=' + nodeId + ' node-label=' + nodeLabel + ' node-children=' + nodeChildren + ' label-select=' + labelSelect +'></div>' +
              '</li>' +
            '</ul>';


          //check tree id, tree model
          // if( treeId && treeModel ) {

            //root node
            if( attrs.angularTreeviewcheck ) {
              // if checkbox clicks,
              scope.changeCB = scope.changeCB || function( selectedNode){
                $('input[type="checkbox"]').change(function(e) {
                  var checked = $(this).prop("checked"),
                      container = $(this).parent(),
                      siblings = container.siblings();

                  container.find('input[type="checkbox"]').prop({
                    indeterminate: false,
                    checked: checked
                  });

                  function checkSiblings(el) {

                    var parent = el.parent().parent().parent(),
                        all = true;

                    el.siblings().each(function() {
                      return all = ($(this).children('input[type="checkbox"]').prop("checked") === checked);
                    });

                    if (all && checked) {

                      parent.children('input[type="checkbox"]').prop({
                        indeterminate: false,
                        checked: checked
                      });

                      checkSiblings(parent);

                    } else if (all && !checked) {

                      parent.children('input[type="checkbox"]').prop("checked", checked);
                      parent.children('input[type="checkbox"]').prop("indeterminate", (parent.find('input[type="checkbox"]:checked').length > 0));
                      checkSiblings(parent);

                    } else {

                      el.parents("li").children('input[type="checkbox"]').prop({
                        indeterminate: true,
                        checked: false
                      });

                    }

                  }

                  checkSiblings(container);
                });
              };
              //if node head clicks,
              scope.selectNodeHead = scope.selectNodeHead || function( selectedNode ){

                //Collapse or Expand
                selectedNode.collapsed = !selectedNode.collapsed;
              };

              //if node label clicks,
              scope[labelSelect] = scope[labelSelect] || function( selectedNode ){

                //remove highlight from previous node
                if( scope.currentNode && scope.currentNode.selected ) {
                  scope.currentNode.selected = undefined;
                }

                if(selectedNode.children && selectedNode.children.length > 0){
                  scope.selectNodeHead(selectedNode);
                }else{
                  //set highlight to selected node
                  selectedNode.selected = 'selected';
                }

                //set currentNode
                scope.currentNode = selectedNode;
              };
            }



            //Rendering template.
            element.html('').append( $compile( template )( scope ) );
          // }
        }
      };
    }]);

});
