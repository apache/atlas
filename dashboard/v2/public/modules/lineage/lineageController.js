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

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'lodash', 'LineageResource', 'd3',
    function($element, $scope, $state, $stateParams, _, LineageResource, d3) {

        $scope.lineageData = LineageResource.get({
            id: $stateParams.id
        }, function(data) {
            var nodes = {};

            function getNode(nodeId) {
                if (!nodes[nodeId]) {
                    var node;
                    if (data.vertices[nodeId]) {
                        node = angular.copy(data.vertices[nodeId]);
                        node.__key = nodeId;
                        node.__name = node['hive_table.name'] || node.__key;
                        node.__tooltip = node['hive_table.description'] || node['HiveLineage.query'];
                    } else {
                        node = {};
                        node.__key = nodeId;
                        node.__tooltip = node.__name = nodeId + ', Node Missing';
                    }
                    nodes[nodeId] = node;
                }
                return nodes[nodeId];
            }

            var edges = [],
                edgeTypes = [];

            angular.forEach(data.edges, function(edge) {
                /* Put the head (edge) inside tail (edge)
                 * Tail is parent
                 * Head is child
                 * */
                var parentNode = getNode(edge.tail);
                edge.source = parentNode;
                edge.target = getNode(edge.head);

                parentNode.__hasChild = true;

                edge.__type = edge.label;
                edgeTypes.push(edge.label);
                edges.push(edge);
            });
            edgeTypes = _.uniq(edgeTypes);
            render(nodes, edges, edgeTypes);
        });

        function render(nodes, links, linkTypes) {
            // Use elliptical arc path segments to doubly-encode directionality.
            function click(node) {
                if (node.guid) {
                    $state.go('details', {
                        id: node.guid
                    }, {
                        location: 'replace'
                    });
                }
            }

            function tick() {
                path.attr("d", linkArc);
                circle.attr("transform", transform);
                text.attr("transform", transform);
            }

            function linkArc(d) {
                var dx = d.target.x - d.source.x,
                    dy = d.target.y - d.source.y,
                    dr = Math.sqrt(dx * dx + dy * dy);
                return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
            }

            function transform(d) {
                return "translate(" + d.x + "," + d.y + ")";
            }

            var width = Math.max($element[0].offsetWidth, 960),
                height = Math.max($element[0].offsetHeight, 350);

            var force = d3.layout.force()
                .nodes(d3.values(nodes))
                .links(links)
                .size([width, height])
                .linkDistance(200)
                .charge(-120)
                .gravity(0.05)
                .on("tick", tick)
                .start();

            var svg = d3.select($element[0]).select('svg')
                .attr("width", width)
                .attr("height", height);

            /* Initialize tooltip */
            var tooltip = d3.tip()
                .attr('class', 'd3-tip')
                .html(function(d) {
                    return '<pre class="alert alert-success">' + d.__tooltip + '</pre>';
                });

            /* Invoke the tip in the context of your visualization */
            svg.call(tooltip);

            // Per-type markers, as they don't inherit styles.
            var defs = svg.append("defs");

            var imageDim = 10;
            defs.append('svg:pattern')
                .attr('id', 'process-image')
                .attr('patternUnits', 'userSpaceOnUse')
                .attr('width', imageDim)
                .attr('height', imageDim)
                .append('svg:image')
                .attr('xlink:href', '/img/process.png')
                .attr('x', 0)
                .attr('y', 0)
                .attr('width', imageDim)
                .attr('height', imageDim);

            defs.selectAll("marker")
                .data(linkTypes)
                .enter().append("marker")
                .attr("id", function(d) {
                    return d;
                })
                .attr("viewBox", "0 -5 10 10")
                .attr("refX", 15)
                .attr("refY", -1.5)
                .attr("markerWidth", 6)
                .attr("markerHeight", 6)
                .attr("orient", "auto")
                .append("path")
                .attr("d", "M0,-5L10,0L0,5");

            var path = svg.append("g").selectAll("path")
                .data(force.links())
                .enter().append("path")
                .attr("class", function(d) {
                    return "link " + d.__type;
                })
                .attr("marker-end", function(d) {
                    return "url(#" + d.__type + ")";
                });

            var circle = svg.append("g").selectAll("circle")
                .data(force.nodes())
                .enter().append("circle")
                .on('click', click)
                .on('mouseover', tooltip.show)
                .on('mouseout', tooltip.hide)
                .attr('class', function(d) {
                    return d.__hasChild ? '' : 'empty';
                })
                .attr("r", function(d) {
                    return d.__hasChild ? 15 : 10;
                })
                .call(force.drag);

            var text = svg.append("g").selectAll("text")
                .data(force.nodes())
                .enter().append("text")
                .attr('dy', '2em')
                .text(function(d) {
                    return d.__name;
                });
        }

    }
]);
