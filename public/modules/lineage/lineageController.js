'use strict';

angular.module('dgc.lineage').controller('LineageController', ['$element', '$scope', '$stateParams', 'LineageResource', 'd3',
    function($element, $scope, $stateParams, LineageResource, d3) {

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
                //.on('click', click)
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

        var width = 960,
            height = 600;

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

        $scope.lineageData = LineageResource.get({
            id: $stateParams.id
        }, function(data) {
            var nodes = {};

            function getNode(nodeId) {
                if (!nodes[nodeId]) {
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
                if (!parentNode.children) {
                    parentNode.children = [];
                }
                var childId = node.head;
                parentNode.children.push(getNode(childId));
            });

            var id = 0,
                returnArray = [];

            angular.forEach(nodes, function(node, key) {
                node.__id = id++;
                node.__key = key;
                node.__name = node['hive_table.name'];
                returnArray.push(node);
            });
            render(returnArray);
        });
    }
]);
