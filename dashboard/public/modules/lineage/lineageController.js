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
                }else{ 
                    $scope.requested = false;
                }
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
        $scope.height = $element[0].offsetHeight;
        $scope.width = $element[0].offsetWidth;

        function render() {
            renderGraph($scope.lineageData, {
                eleObj : $element,
                element: $element[0],
                height: $scope.height,
                width: $scope.width
            });
            $scope.rendered = true;
        }

        $scope.onReset = function(){
            renderGraph($scope.lineageData, {
                eleObj : $element,
                element: $element[0],
                height: $scope.height,
                width: $scope.width
            }); 
        };

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
            // ************** Generate the tree diagram  *****************
            var element = d3.select(container.element),  
                widthg = Math.max(container.width, 960),
                heightg = Math.max(container.height, 500),

                totalNodes = 0,
                maxLabelLength = 0,
                selectedNode = null,
                draggingNode = null,
                dragListener = null,
                dragStarted = true,
                domNode = null,
                multiParents = null,
                nodes = null,
                tooltip = null,
                node = null,  
                i = 0,
                duration = 750,
                root,
                depthwidth = 10;
                

            var viewerWidth = widthg - 15,
                viewerHeight = heightg;
 
             var tree = d3.layout.tree().nodeSize([100, 200]);
                /*.size([viewerHeight, viewerWidth]);*/

    container.eleObj.find(".graph").html('');    
    container.eleObj.find("svg").remove();

    // define a d3 diagonal projection for use by the node paths later on.
    var diagonal = d3.svg.diagonal()
        .projection(function(d) {
            return [d.y, d.x];
        });

    // A recursive helper function for performing some setup by walking through all nodes

    function visit(parent, visitFn, childrenFn) {
        if (!parent) return;

        visitFn(parent);

        var children = childrenFn(parent);
        if (children) {
            var count = children.length;
            for (var i = 0; i < count; i++) {
                visit(children[i], visitFn, childrenFn);
            }
        }
    }

    // Call visit function to establish maxLabelLength
    visit(data, function(d) {
        totalNodes++;
        maxLabelLength = Math.max(d.name.length, maxLabelLength);

    }, function(d) {
        return d.children && d.children.length > 0 ? d.children : null;
    });


    // sort the tree according to the node names

    function sortTree() {
        tree.sort(function(a, b) {
            return b.name.toLowerCase() < a.name.toLowerCase() ? 1 : -1;
        });
    }
    // Sort the tree initially incase the JSON isn't in a sorted order.
    sortTree(); 
     
    // Define the zoom function for the zoomable tree

    function zoom() {
        svgGroup.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
    }


    // define the zoomListener which calls the zoom function on the "zoom" event constrained within the scaleExtents
    var zoomListener = d3.behavior.zoom().scaleExtent([0.1, 3]).on("zoom", zoom);
     /* Initialize tooltip */
    tooltip = d3.tip()
        .attr('class', 'd3-tip')
        .html(function(d) {
            return '<pre class="alert alert-success">' + d.name + '</pre>';
        });

    // define the baseSvg, attaching a class for styling and the zoomListener
    var baseSvg = element.append('svg')        
        .attr("width", viewerWidth)
        .attr("height", viewerHeight)
        .attr("class", "overlay")
        .call(zoomListener)
        .call(tooltip);


    // Define the drag listeners for drag/drop behaviour of nodes.
    dragListener = d3.behavior.drag()
        .on("dragstart", function(d) {
            if (d ===root) {
                return;
            }
            dragStarted = true;
            nodes = tree.nodes(d);
            d3.event.sourceEvent.stopPropagation();
            // it's important that we suppress the mouseover event on the node being dragged. Otherwise it will absorb the mouseover event and the underlying node will not detect it d3.select(this).attr('pointer-events', 'none');
        }) 
        .on("dragend", function(d) {
            if (d ===root) {
                return;
            }
            domNode = this;
            if (selectedNode) {
                // now remove the element from the parent, and insert it into the new elements children
                var index = draggingNode.parent.children.indexOf(draggingNode);
                if (index > -1) {
                    draggingNode.parent.children.splice(index, 1);
                }
                if (typeof selectedNode.children !== 'undefined' || typeof selectedNode._children !== 'undefined') {
                    if (typeof selectedNode.children !== 'undefined') {
                        selectedNode.children.push(draggingNode);
                    } else {
                        selectedNode._children.push(draggingNode);
                    }
                } else {
                    selectedNode.children = [];
                    selectedNode.children.push(draggingNode);
                }
                // Make sure that the node being added to is expanded so user can see added node is correctly moved
                expand(selectedNode);
                sortTree();
                endDrag();
            } else {
                endDrag();
            }
        });

    function endDrag() {
        selectedNode = null;
        d3.selectAll('.ghostCircle').attr('class', 'ghostCircle');
        d3.select(domNode).attr('class', 'node');
        // now restore the mouseover event or we won't be able to drag a 2nd time
        d3.select(domNode).select('.ghostCircle').attr('pointer-events', '');
        updateTempConnector();
        if (draggingNode !== null) {
            update(root);
            centerNode(draggingNode);
            draggingNode = null;
        }
    }


    function expand(d) {
        if (d._children) {
            d.children = d._children;
            d.children.forEach(expand);
            d._children = null;
        }
    } 

    // Function to update the temporary connector indicating dragging affiliation
    var updateTempConnector = function() {
        var data = [];
        if (draggingNode !== null && selectedNode !== null) {
            // have to flip the source coordinates since we did this for the existing connectors on the original tree
            data = [{
                source: {
                    x: selectedNode.y0,
                    y: selectedNode.x0
                },
                target: {
                    x: draggingNode.y0,
                    y: draggingNode.x0
                }
            }];
        }
        var link = svgGroup.selectAll(".templink").data(data);

        link.enter().append("path")
            .attr("class", "templink")
            .attr("d", d3.svg.diagonal())
            .attr('pointer-events', 'none');

        link.attr("d", d3.svg.diagonal());

        link.exit().remove();
    };

    // Function to center node when clicked/dropped so node doesn't get lost when collapsing/moving with large amount of children.

    function centerNode(source) {
        var scale =  (depthwidth === 10) ? zoomListener.scale() : 0.4;
        var x = -source.y0;
        var y = -source.x0;
        x = x * scale + 150;
        y = y * scale + viewerHeight / 2;
        d3.select('g').transition()
            .duration(duration)
            .attr("transform", "translate(" + x + "," + y + ")scale(" + scale + ")");
        zoomListener.scale(scale);
        zoomListener.translate([x, y]);
    }

    // Toggle children function

    function toggleChildren(d) {
        if (d.children) {
            d._children = d.children;
            d.children = null;
        } else if (d._children) {
            d.children = d._children;
            d._children = null;
        }
        return d;
    }

    // Toggle children on click.

    function click(d) {
        if (d3.event.defaultPrevented) return; // click suppressed
        d = toggleChildren(d);
        update(d);
        //centerNode(d);
    }

    //arrow
            baseSvg.append("svg:defs")
                .append("svg:marker")
                .attr("id", "arrow")
                .attr("viewBox", "0 0 10 10")
                .attr("refX", 22)
                .attr("refY", 5)
                .attr("markerUnits", "strokeWidth")
                .attr("markerWidth", 6)
                .attr("markerHeight", 9)
                .attr("orient", "auto")
                .append("svg:path")
                .attr("d", "M 0 0 L 10 5 L 0 10 z");

            //marker for input type graph
            baseSvg.append("svg:defs")
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

    function update(source) {
        // Compute the new height, function counts total children of root node and sets tree height accordingly.
        // This prevents the layout looking squashed when new nodes are made visible or looking sparse when nodes are removed
        // This makes the layout more consistent.
        var levelWidth = [1];
        var childCount = function(level, n) {

            if (n.children && n.children.length > 0) {
                if (levelWidth.length <= level + 1) levelWidth.push(0);

                levelWidth[level + 1] += n.children.length;
                n.children.forEach(function(d) {
                    childCount(level + 1, d);
                });
            }
        };
        childCount(0, root);
        tree = tree.nodeSize([50, 100]); 

        // Compute the new tree layout.
        var nodes = tree.nodes(root).reverse(),
            links = tree.links(nodes);

        // Set widths between levels based on maxLabelLength.
        nodes.forEach(function(d) {
            if(levelWidth.length > 1 && depthwidth === 10){
               for(var o=0; o < levelWidth.length; o++){
                  if(levelWidth[o] > 4 ) { depthwidth = 70;  break;}
               }
            }  
            var maxLebal = maxLabelLength;
            if(depthwidth === 10) { maxLebal = 20;}
            d.y = (d.depth * (maxLebal * depthwidth));           
        }); 

        // Update the nodes…
        node = svgGroup.selectAll("g.node")
            .data(nodes, function(d) {
                return d.id || (d.id = ++i);
            });

        // Enter any new nodes at the parent's previous position.
        var nodeEnter = node.enter().append("g")
            .call(dragListener)
            .attr("class", "node")
            .attr("transform", function() {
                return "translate(" + source.y0 + "," + source.x0 + ")";
            })
            .on('click', click);
 
         nodeEnter.append("image")
            .attr("class","nodeImage")
            .attr("xlink:href", function(d) { 
                return d.type === 'Table' ? '../img/tableicon.png' : '../img/process.png';
            })
            .on('mouseover', function(d) {
                if (d.type === 'LoadProcess' || 'Table') {
                    tooltip.show(d);
                }
            })
            .on('mouseout', function(d) {
                if (d.type === 'LoadProcess' || 'Table') {
                    tooltip.hide(d);
                }
            })
            .attr("x", "-18px")
            .attr("y", "-18px")
            .attr("width", "34px")
            .attr("height", "34px");    

        nodeEnter.append("text")
            .attr("x", function(d) {
                return d.children || d._children ? -10 : 10;
            })
            .attr("dx", function (d) { return d.children ? 50 : -50; })
            .attr("dy", -24) 
            .attr('class', 'place-label')
            .attr("text-anchor", function(d) {
                return d.children || d._children ? "end" : "start";
            })
            .text(function(d) {
                var nameDis = (d.name.length > 15) ? d.name.substring(0,15) + "..." : d.name;
                $(this).attr('title', d.name);
                return nameDis;
            })
            .style("fill-opacity", 0); 

        // Update the text to reflect whether node has children or not.
        node.select('text')
            .attr("x", function(d) {
                return d.children || d._children ? -10 : 10;
            })
            .attr("text-anchor", function(d) {
                return d.children || d._children ? "end" : "start";
            })
            .text(function(d) {
                var nameDis = (d.name.length > 15) ? d.name.substring(0,15) + "..." : d.name;
                $(this).attr('title', d.name);
                return nameDis;
            });

        // Change the circle fill depending on whether it has children and is collapsed
        // Change the circle fill depending on whether it has children and is collapsed
        node.select("image.nodeImage")
            .attr("r", 4.5)
            .attr("xlink:href", function(d) { 
                if(d._children){
                    return d.type === 'Table' ? '../img/tableicon1.png' : '../img/process1.png';
                }
                return d.type === 'Table' ? '../img/tableicon.png' : '../img/process.png';
            });


        // Transition nodes to their new position.
        var nodeUpdate = node.transition()
            .duration(duration)
            .attr("transform", function(d) {
                return "translate(" + d.y + "," + d.x  + ")";
            });

        // Fade the text in
        nodeUpdate.select("text")
            .style("fill-opacity", 1);

        // Transition exiting nodes to the parent's new position.
        var nodeExit = node.exit().transition()
            .duration(duration)
            .attr("transform", function() {
                return "translate(" + source.y + "," + source.x + ")";
            })
            .remove();

        nodeExit.select("circle")
            .attr("r", 0);

        nodeExit.select("text")
            .style("fill-opacity", 0);

        // Update the links…
        var link = svgGroup.selectAll("path.link")
            .data(links, function(d) {
                return d.target.id;
            });

        // Enter any new links at the parent's previous position.
        link.enter().insert("path", "g")
            .attr("class", "link")
            .style('stroke', 'green') 
            .attr("d", function() {
                var o = {
                    x: source.x0,
                    y: source.y0
                };
                return diagonal({
                    source: o,
                    target: o
                });
            });

        // Transition links to their new position.
        link.transition()
            .duration(duration)
            .attr("d", diagonal);

        // Transition exiting nodes to the parent's new position.
        link.exit().transition()
            .duration(duration)
            .attr("d", function() {
                var o = {
                    x: source.x,
                    y: source.y
                };
                return diagonal({
                    source: o,
                    target: o
                });
            })
            .remove();

        // Stash the old positions for transition.
        nodes.forEach(function(d) {
            d.x0 = d.x;
            d.y0 = d.y;
        });

        if ($scope.type === 'inputs') {
            link.attr("marker-start", "url(#input-arrow)"); //if input
        }  else {
            link.attr("marker-end", "url(#arrow)"); //if input
        }
    }

    // Append a group which holds all nodes and which the zoom Listener can act upon.
    var svgGroup = baseSvg.append("g")
                    .attr("transform", "translate(120 ," + heightg/2 + ")");

    // Define the root
    root = data;
    root.x0 = viewerHeight / 2;
    root.y0 = 0;

    // Layout the tree initially and center on the root node.
    update(root);
    centerNode(root);
    
    var couplingParent1 = tree.nodes(root).filter(function(d) {
            return d.name === 'cluster';
        })[0];
    var couplingChild1 = tree.nodes(root).filter(function(d) {
            return d.name === 'JSONConverter';
        })[0];
    
    multiParents = [{
                    parent: couplingParent1,
                    child: couplingChild1
                }];
    
    multiParents.forEach(function() {
            svgGroup.append("path", "g"); 
        }); 

           
        }

    }
]);
