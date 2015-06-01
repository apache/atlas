/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'lodash', 'LineageResource', 'd3', 'dagreD3',
    function($element, $scope, $state, $stateParams, _, LineageResource, d3, dagreD3) {

        function getLineageData(tableData, callRender) {
            LineageResource.get({
                tableName: tableData.tableName,
                type: tableData.type
            }, function lineageSuccess(response) {
                if (!_.isEmpty(response.results.values.vertices)) {
                    $scope.lineageData = transformData(response.results);
                    if (callRender) {
                        render();
                    }
                }
                $scope.requested = false;
            });
        }

        $scope.type = $element.parent().attr('data-table-type');
        $scope.requested = false;

        function render() {
            renderGraph($scope.lineageData, {
                element: $element[0],
                height: $element[0].offsetHeight,
                width: $element[0].offsetWidth
            });
            $scope.rendered = true;
        }

        $scope.$on('render-lineage', function(event, lineageData) {
            if (lineageData.type === $scope.type) {
                if (!$scope.lineageData) {
                    if (!$scope.requested) {
                        getLineageData(lineageData, true);
                        $scope.requested = true;
                    }
                } else {
                    render();
                }
            }
        });

        function transformData(metaData) {
            var nodes = [];
            var name, guid;
            var nodeGuids = Object.keys(metaData.values.vertices);
            for (var index in nodeGuids) {
                name = metaData.values.vertices[nodeGuids[index]].values.name;
                guid = nodeGuids[index];
                nodes.push({
                    guid: guid,
                    label: name,
                    shape: 'rect'
                });
            }

            var edges = [];
            var parent;
            var child;
            var edgesParents = Object.keys(metaData.values.edges);
            for (index in edgesParents) {
                parent = edgesParents[index];
                for (var j = 0; j < metaData.values.edges[parent].length; j++) {
                    child = metaData.values.edges[parent][j];
                    if (!metaData.values.vertices.hasOwnProperty(child)) {
                        nodes.push({
                            guid: child,
                            label: 'Load Process',
                            shape: 'circle'
                        });
                    }
                    edges.push({
                        parent: parent,
                        child: child
                    });
                }
            }
            return {
                nodes: nodes,
                edges: edges
            };
        }

        function renderGraph(data, container) {
            // Create a new directed graph
            var g = new dagreD3
                .graphlib
                .Graph()
                .setGraph({
                    rankdir: 'LR'
                });

            // Automatically label each of the nodes
            //g.setNode('DB (sales)', { label: 'Sales DB',  width: 144, height: 100 })
            //states.forEach(function(state) { g.setNode(state, { label: state }); });

            _.forEach(data.nodes, function(node) {
                g.setNode(node.guid, {
                    label: node.label,
                    shape: node.shape
                });
            });

            _.forEach(data.edges, function(edge) {
                g.setEdge(edge.parent, edge.child, {
                    label: ''
                });
            });

            // Set some general styles
            g.nodes().forEach(function(v) {
                var node = g.node(v);
                node.rx = node.ry = 5;
            });

            var element = d3.select(container.element),
                width = Math.max(container.width, 960),
                height = Math.max(container.height, 350),
                inner = element.select('svg')
                .attr('width', width)
                .attr('height', height)
                .select('g');

            // Create the renderer
            var render = new dagreD3.render();

            // Run the renderer. This is what draws the final graph.
            render(inner, g);

            // Center the graph
            //var initialScale = 0.75;
            //  zoom
            //     .translate([(container.attr('width') - g.graph().width * initialScale) / 2, 20])
            //    .scale(initialScale)
            //    .event(container);
            //container.attr('height', g.graph().height * initialScale + 90);
        }

    }
]);
