/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'lodash', 'LineageResource', 'd3',
    function($element, $scope, $state, $stateParams, _, LineageResource, d3) {

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
            var edges = metaData.values.edges,
                vertices = metaData.values.vertices,
                nodes = {};

            function getNode(guid) {
                var vertex = {
                    guid: guid,
                    name: vertices.hasOwnProperty(guid) ? vertices[guid].values.name : 'Load Process-Added'
                };
                if (!nodes.hasOwnProperty(guid)) {
                    nodes[guid] = vertex;
                }
                return nodes[guid];
            }

            function attachParent(edge, node) {
                edge.forEach(function eachPoint(childGuid) {
                    var childNode = getNode(childGuid);
                    node.children = node.children || [];
                    node.children.push(childNode);
                    childNode.parent = node.guid;
                });
            }

            /* Loop through all edges and attach them to correct parent */
            for (var guid in edges) {
                var edge = edges[guid],
                    node = getNode(guid);

                /* Attach parent to each endpoint of edge */
                attachParent(edge, node);
            }

            /* Return the first node w/o parent, this is root node*/
            return _.find(nodes, function(node) {
                return !node.hasOwnProperty('parent');
            });
        }

        function renderGraph(data, container) {
            // ************** Generate the tree diagram	 *****************
            var element = d3.select(container.element),
                width = Math.max(container.width, 960),
                height = Math.max(container.height, 350);

            var margin = {
                top: 20,
                right: 120,
                bottom: 20,
                left: 120
            };
            width = width - margin.right - margin.left;
            height = height - margin.top - margin.bottom;

            var i = 0;

            var tree = d3.layout.tree()
                .size([height, width]);

            var diagonal = d3.svg.diagonal()
                .projection(function(d) {
                    return [d.y, d.x];
                });

            var svg = element.select('svg')
                .attr('width', width + margin.right + margin.left)
                .attr('height', height + margin.top + margin.bottom)
                .select('g')
                .attr('transform',
                    'translate(' + margin.left + ',' + margin.top + ')');

            var root = data;

            function update(source) {

                // Compute the new tree layout.
                var nodes = tree.nodes(source).reverse(),
                    links = tree.links(nodes);

                // Normalize for fixed-depth.
                nodes.forEach(function(d) {
                    d.y = d.depth * 180;
                });

                // Declare the nodes…
                var node = svg.selectAll('g.node')
                    .data(nodes, function(d) {
                        return d.id || (d.id = ++i);
                    });

                // Enter the nodes.
                var nodeEnter = node.enter().append('g')
                    .attr('class', 'node')
                    .attr('transform', function(d) {
                        return 'translate(' + d.y + ',' + d.x + ')';
                    });

                nodeEnter.append('image')
                    .attr('xlink:href', function(d) {
                        return d.icon;
                    })
                    .attr('x', '-12px')
                    .attr('y', '-12px')
                    .attr('width', '24px')
                    .attr('height', '24px');

                nodeEnter.append('text')
                    .attr('x', function(d) {
                        return d.children || d._children ?
                            (15) * -1 : +15;
                    })
                    .attr('dy', '.35em')
                    .attr('text-anchor', function(d) {
                        return d.children || d._children ? 'end' : 'start';
                    })
                    .text(function(d) {
                        return d.name;
                    })
                    .style('fill-opacity', 1);

                // Declare the links…
                var link = svg.selectAll('path.link')
                    .data(links, function(d) {
                        return d.target.id;
                    });

                // Enter the links.
                link.enter().insert('path', 'g')
                    .attr('class', 'link')
                    //.style('stroke', function(d) { return d.target.level; })
                    .style('stroke', '#000')
                    .attr('d', diagonal);

            }

            update(root);
        }

    }
]);
