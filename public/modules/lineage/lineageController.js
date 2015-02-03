'use strict';

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$state', '$stateParams', 'LineageResource', 'd3',
    function($element, $scope, $state, $stateParams, LineageResource, d3) {

        function render(nodes) {
            var links = d3.layout.tree().links(nodes);
            // Restart the force layout.
            force.nodes(nodes)
                .links(links)
                .start();

            // Update links.
            link = link.data(links, function(d) {
                return d.target.__id;
            });

            link.exit().remove();

            link.enter().insert('line', '.node')
                .attr('class', 'link');

            // Update nodes.
            node = node.data(nodes, function(d) {
                return d.__id;
            });

            node.exit().remove();

            var nodeEnter = node.enter().append('g')
                .attr('class', 'node')
                .on('click', click)
                .on('mouseover', tooltip.show)
                .on('mouseout', tooltip.hide)
                .call(force.drag);

            nodeEnter.append('circle')
                .attr('class', function(d) {
                    return d.children ? '' : 'empty';
                }).attr('r', 9);

            nodeEnter.append('text')
                .attr('dy', '2em')
                .text(function(d) {
                    //return d.name;
                    return d.__name || d.__key;
                });

            /*node.select('circle')
             .style('fill', color);*/
        }

        function click(node) {
            $state.go('details', {
                id: node.guid
            }, {
                location: 'replace'
            });
        }

        function tick() {
            link.attr('x1', function(d) {
                return d.source.x;
            }).attr('y1', function(d) {
                return d.source.y;
            }).attr('x2', function(d) {
                return d.target.x;
            }).attr('y2', function(d) {
                return d.target.y;
            });

            node.attr('transform', function(d) {
                return 'translate(' + d.x + ',' + d.y + ')';
            });
        }

        var width = Math.max($element[0].offsetWidth, 960),
            height = Math.max($element[0].offsetHeight, 350);

        var force = d3.layout.force()
            .linkDistance(200)
            .charge(-120)
            .gravity(0.05)
            .size([width, height])
            .on('tick', tick);

        var svg = d3.select($element[0]).select('svg')
            .attr('width', width)
            .attr('height', height);

        var link = svg.selectAll('.link'),
            node = svg.selectAll('.node');

        /* Initialize tooltip */
        var tooltip = d3.tip()
            .attr('class', 'd3-tip')
            .html(function(d) {
                return '<pre class="alert alert-success">' + d.__tooltip + '</pre>';
            });

        /* Invoke the tip in the context of your visualization */
        svg.call(tooltip);

        $scope.lineageData = LineageResource.get({
            id: $stateParams.id
        }, function(data) {
            var nodes = {};

            function getNode(nodeId) {
                if (!nodes[nodeId] && data.vertices[nodeId]) {
                    nodes[nodeId] = angular.copy(data.vertices[nodeId]);
                }
                return nodes[nodeId];
            }

            angular.forEach(data.edges, function(node) {
                /* Put the head (node) inside tail (node)
                 * Tail is parent
                 * Head is child
                 * */
                var parentId = node.tail,
                    parentNode = getNode(parentId);
                if (!parentNode) {
                    console.log('Parent Node not found id', parentId);
                } else {
                    var childId = node.head,
                        childNode = getNode(childId);
                    if (childNode) {
                        if (!parentNode.children) {
                            parentNode.children = [];
                        }
                        parentNode.children.push(childNode);
                    } else {
                        console.log('Child Node not found id', childId);
                    }
                }
            });

            var id = 0,
                returnArray = [];

            angular.forEach(nodes, function(node, key) {
                node.__id = id++;
                node.__key = key;
                node.__name = node['hive_table.name'];
                node.__tooltip = node['hive_table.description'] || node['HiveLineage.query'];
                returnArray.push(node);
            });
            render(returnArray);
        });
    }
]);
