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

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'lodash', 'LineageResource', 'd3', 'DetailsResource', '$q',
    function($element, $scope, $state, $stateParams, _, LineageResource, d3, DetailsResource, $q) {
        var guidsList = [];

        function getLineageData(tableData, callRender) {
            LineageResource.get({
                tableName: tableData.tableName,
                type: tableData.type
            }, function lineageSuccess(response) {
                if (!_.isEmpty(response.results.values.vertices)) {
                    var allGuids = loadProcess(response.results.values.edges, response.results.values.vertices);
                    allGuids.then(function(res) {
                        guidsList = res;
                        $scope.lineageData = transformData(response.results);
                        if (callRender) {
                            render();
                        }
                    });
                }
                $scope.requested = false;
            });
        }

        function loadProcess(edges, vertices) {

            var urlCalls = [];
            var deferred = $q.defer();
            for (var guid in edges) {
                if (!vertices.hasOwnProperty(guid)) {
                    urlCalls.push(DetailsResource.get({
                        id: guid
                    }).$promise);
                }

            }
            $q.all(urlCalls)
                .then(function(results) {
                    deferred.resolve(results);
                });
            return deferred.promise;
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
                var name, type, tip;
                if (vertices.hasOwnProperty(guid)) {
                    name = vertices[guid].values.name;
                    type = vertices[guid].values.vertexId.values.typeName;
                } else {
                    var loadProcess = getLoadProcessTypes(guid);
                    if (typeof loadProcess !== "undefined") {
                        name = loadProcess.name;
                        type = loadProcess.typeName;
                        tip = loadProcess.tip;
                    } else {
                        name = 'Load Process';
                        type = 'Load Process';
                    }
                }
                var vertex = {
                    guid: guid,
                    name: name,
                    type: type,
                    tip: tip
                };
                if (!nodes.hasOwnProperty(guid)) {
                    nodes[guid] = vertex;
                }
                return nodes[guid];
            }

            function getLoadProcessTypes(guid) {
                var procesRes = [];
                angular.forEach(guidsList, function(value) {
                    if (value.id.id === guid) {
                        procesRes.name = value.values.name;
                        procesRes.typeName = value.typeName;
                        procesRes.tip = value.values.queryText;
                    }
                });
                return procesRes;
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
                top: 100,
                right: 80,
                bottom: 30,
                left: 80
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

            /* Initialize tooltip */
            var tooltip = d3.tip()
                .attr('class', 'd3-tip')
                .html(function(d) {
                    return '<pre class="alert alert-success">' + d.tip + '</pre>';
                });

            var svg = element.select('svg')
                .attr('width', width + margin.right + margin.left)
                .attr('height', height + margin.top + margin.bottom)
                /* Invoke the tip in the context of your visualization */
                .call(tooltip)
                .select('g')
                .attr('transform',
                    'translate(' + margin.left + ',' + margin.right + ')');
            //arrow
            svg.append("svg:defs").append("svg:marker").attr("id", "arrow").attr("viewBox", "0 0 10 10").attr("refX", 26).attr("refY", 5).attr("markerUnits", "strokeWidth").attr("markerWidth", 6).attr("markerHeight", 9).attr("orient", "auto").append("svg:path").attr("d", "M 0 0 L 10 5 L 0 10 z");

            //marker for input type graph
            svg.append("svg:defs")
                .append("svg:marker")
                .attr("id", "input-arrow")
                .attr("viewBox", "0 0 10 10")
                .attr("refX", -15)
                .attr("refY", 5)
                .attr("markerUnits", "strokeWidth")
                .attr("markerWidth", 6)
                .attr("markerHeight", 9)
                .attr("orient", "auto")
                .append("svg:path")
                .attr("d", "M -2 5 L 8 0 L 8 10 z");

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

                nodeEnter.append("image")
                    .attr("xlink:href", function(d) {
                        //return d.icon;
                        return d.type === 'Table' ? '../img/tableicon.png' : '../img/process.png';
                    })
                    .on('mouseover', function(d) {
                        if (d.type === 'LoadProcess') {
                            tooltip.show(d);
                        }
                    })
                    .on('mouseout', function(d) {
                        if (d.type === 'LoadProcess') {
                            tooltip.hide(d);
                        }
                    })
                    .attr("x", "-18px")
                    .attr("y", "-18px")
                    .attr("width", "34px")
                    .attr("height", "34px");

                nodeEnter.append('text')
                    .attr('x', function(d) {
                        return d.children || d._children ?
                            (5) * -1 : +15;
                    })
                    .attr('dy', '-1.75em')
                    .attr('text-anchor', function(d) {
                        return d.children || d._children ? 'middle' : 'middle';
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

                link.enter().insert('path', 'g')
                    .attr('class', 'link')
                    //.style('stroke', function(d) { return d.target.level; })
                    .style('stroke', 'green')
                    .attr('d', diagonal);

                if ($scope.type === 'inputs') {
                    link.attr("marker-start", "url(#input-arrow)"); //if input
                } else {
                    link.attr("marker-end", "url(#arrow)"); //if input
                }

            }

            update(root);
        }

    }
]);
