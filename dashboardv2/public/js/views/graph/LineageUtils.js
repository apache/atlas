/**
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

define(['require', ''], function(require) {
    'use strict';
    var LinegaeUtils = {};
    LinegaeUtils.DragNode = function(options) {
        var that = this,
            g = options.g,
            svg = options.svg,
            guid = options.guid,
            edgePathEl = options.edgeEl;
        return {
            init: function() {
                var that = this;
                //give IDs to each of the nodes so that they can be accessed
                svg.selectAll("g.node rect")
                    .attr("id", function(d) {
                        return "node" + d;
                    });
                svg.selectAll("g.edgePath path")
                    .attr("id", function(e) {
                        return e.v + "-" + e.w;
                    });
                svg.selectAll("g.edgeLabel g")
                    .attr("id", function(e) {
                        return 'label_' + e.v + "-" + e.w;
                    });

                g.nodes().forEach(function(v) {
                    var node = g.node(v);
                    node.customId = "node" + v;
                })
                g.edges().forEach(function(e) {
                    var edge = g.edge(e.v, e.w);
                    edge.customId = e.v + "-" + e.w
                });
                var nodeDrag = d3.behavior.drag()
                    .on("dragstart", this.dragstart)
                    .on("drag", function(d) { that.dragmove(this, d) });

                var edgeDrag = d3.behavior.drag()
                    .on("dragstart", this.dragstart)
                    .on('drag', function(d) {
                        that.translateEdge(g.edge(d.v, d.w), d3.event.dx, d3.event.dy);
                        $('#' + g.edge(d.v, d.w).customId).attr('d', that.calcPoints(d));
                    });

                nodeDrag.call(svg.selectAll("g.node"));
                edgeDrag.call(svg.selectAll("g.edgePath"));
            },
            dragstart: function(d) {
                d3.event.sourceEvent.stopPropagation();
            },
            dragmove: function(elem, d) {
                var that = this,
                    node = d3.select(elem),
                    selectedNode = g.node(d),
                    prevX = selectedNode.x,
                    prevY = selectedNode.y;

                selectedNode.x += d3.event.dx;
                selectedNode.y += d3.event.dy;
                node.attr('transform', 'translate(' + selectedNode.x + ',' + selectedNode.y + ')');

                var dx = selectedNode.x - prevX,
                    dy = selectedNode.y - prevY;

                g.edges().forEach(function(e) {
                    if (e.v == d || e.w == d) {
                        var edge = g.edge(e.v, e.w);
                        that.translateEdge(g.edge(e.v, e.w), dx, dy);
                        $('#' + edge.customId).attr('d', that.calcPoints(e));
                        var label = $('#label_' + edge.customId);
                        var xforms = label.attr('transform');
                        if (xforms != "") {
                            var parts = /translate\(\s*([^\s,)]+)[ ,]?([^\s,)]+)?/.exec(xforms),
                                X = parseInt(parts[1]) + dx,
                                Y = parseInt(parts[2]) + dy;
                            if (isNaN(Y)) {
                                Y = dy;
                            }
                            label.attr('transform', 'translate(' + X + ',' + Y + ')');
                        }
                    }
                });
                LinegaeUtils.refreshGraphForIE({ "edgeEl": edgePathEl })
            },
            translateEdge: function(e, dx, dy) {
                e.points.forEach(function(p) {
                    p.x = p.x + dx;
                    p.y = p.y + dy;
                });
            },
            calcPoints: function(e) {
                var edge = g.edge(e.v, e.w),
                    tail = g.node(e.v),
                    head = g.node(e.w),
                    points = edge.points.slice(1, edge.points.length - 1),
                    afterslice = edge.points.slice(1, edge.points.length - 1);
                points.unshift(this.intersectRect(tail, points[0]));
                points.push(this.intersectRect(head, points[points.length - 1]));
                return d3.svg.line()
                    .x(function(d) {
                        return d.x;
                    })
                    .y(function(d) {
                        return d.y;
                    })
                    .interpolate("basis")
                    (points);
            },
            intersectRect: function(node, point) {
                var that = this,
                    x = node.x,
                    y = node.y,
                    dx = point.x - x,
                    dy = point.y - y,
                    w = node.id == guid ? 24 : 21,
                    h = node.id == guid ? 24 : 21,
                    sx = 0,
                    sy = 0;

                if (Math.abs(dy) * w > Math.abs(dx) * h) {
                    // Intersection is top or bottom of rect.
                    if (dy < 0) {
                        h = -h;
                    }
                    sx = dy === 0 ? 0 : h * dx / dy;
                    sy = h;
                } else {
                    // Intersection is left or right of rect.
                    if (dx < 0) {
                        w = -w;
                    }
                    sx = w;
                    sy = dx === 0 ? 0 : w * dy / dx;
                }
                return {
                    x: x + sx,
                    y: y + sy
                };
            },
        }
    }
    LinegaeUtils.refreshGraphForSafari = function(options) {
        var edgePathEl = options.edgeEl,
            IEGraphRenderDone = 0;
        edgePathEl.each(function(argument) {
            var eleRef = this,
                childNode = $(this).find('pattern');
            setTimeout(function(argument) {
                $(eleRef).find('defs').append(childNode);
            }, 500);
        });
    }
    LinegaeUtils.refreshGraphForIE = function(options) {
        var edgePathEl = options.edgeEl,
            IEGraphRenderDone = 0;
        edgePathEl.each(function(argument) {
            var childNode = $(this).find('marker');
            $(this).find('marker').remove();
            var eleRef = this;
            ++IEGraphRenderDone;
            setTimeout(function(argument) {
                $(eleRef).find('defs').append(childNode);
                --IEGraphRenderDone;
                if (IEGraphRenderDone === 0) {
                    this.$('.fontLoader').hide();
                    this.$('svg').fadeTo(1000, 1)
                }
            }, 1000);
        });
    }
    LinegaeUtils.centerNode = function(options) {
        var nodeID = options.guid,
            svg = options.svg,
            g = options.g,
            afterCenterZoomed = options.afterCenterZoomed,
            zoom = d3.behavior.zoom(),
            svgGroup = svg.find("g"),
            edgePathEl = options.edgeEl,
            zoomBind = function() {
                svgGroup.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
            },
            selectedNode = $(svg).find("g.nodes").find('>g#' + nodeID);
        return {
            init: function() {
                if (selectedNode.length > 0) {
                    selectedNode = selectedNode;
                    var matrix = selectedNode.attr('transform').replace(/[^0-9\-.,]/g, '').split(',');
                    if (platform.name === "IE" || platform.name === "Microsoft Edge") {
                        var matrix = selectedNode.attr('transform').replace(/[a-z\()]/g, '').split(' ');
                    }
                    var x = matrix[0],
                        y = matrix[1];
                } else {
                    selectedNode = $(svg).find("g.nodes").find('g').eq(1);
                    var x = g.graph().width / 2,
                        y = g.graph().height / 2;

                }
                var viewerWidth = $(svg).width(),
                    viewerHeight = $(svg).height(),
                    gBBox = d3.select('g').node().getBBox(),
                    zoomListener = zoom.scaleExtent([0.01, 50]).on("zoom", zoomBind),
                    scale = 1.2,
                    xa = -((x * scale) - (viewerWidth / 2)),
                    ya = -((y * scale) - (viewerHeight / 2));

                zoom.translate([xa, ya]);
                d3.select('g').transition()
                    .duration(350)
                    .attr("transform", "translate(" + xa + "," + ya + ")scale(" + scale + ")");
                zoomListener.scale(scale);
                zoomListener.translate([xa, ya]);
                zoom.scale(scale);
                afterCenterZoomed({ newScale: scale, newTranslate: [xa, ya] });
                LinegaeUtils.refreshGraphForIE({ "edgeEl": edgePathEl })
            }
        }
    }
    LinegaeUtils.onHoverFade = function(options) {
        var opacity = options.opacity,
            d = options.selectedNode,
            nodesToHighlight = options.highlight,
            svg = options.svg,
            isConnected = function(a, b, o) {
                if (a === o || (b && b.length && b.indexOf(o) != -1)) {
                    return true;
                }
            },
            node = svg.selectAll('.node'),
            path = svg.selectAll('.edgePath');
        return {
            init: function() {
                node.classed("hover-active", function(selectedNode, i, nodes) {
                    if (isConnected(d, nodesToHighlight, selectedNode)) {
                        return true;
                    } else {
                        return false;
                    }
                });
                path.classed('hover-active-node', function(c) {
                    var _thisOpacity = c.v === d || c.w === d ? 1 : 0;
                    if (_thisOpacity) {
                        return true;
                    } else {
                        return false;
                    }
                });

            }
        }

    }
    LinegaeUtils.base64Encode = function(options) {
        var str = options.data,
            CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
            out = "",
            i = 0,
            len = str.length,
            c1, c2, c3;
        while (i < len) {
            c1 = str.charCodeAt(i++) & 0xff;
            if (i == len) {
                out += CHARS.charAt(c1 >> 2);
                out += CHARS.charAt((c1 & 0x3) << 4);
                out += "==";
                break;
            }
            c2 = str.charCodeAt(i++);
            if (i == len) {
                out += CHARS.charAt(c1 >> 2);
                out += CHARS.charAt(((c1 & 0x3) << 4) | ((c2 & 0xF0) >> 4));
                out += CHARS.charAt((c2 & 0xF) << 2);
                out += "=";
                break;
            }
            c3 = str.charCodeAt(i++);
            out += CHARS.charAt(c1 >> 2);
            out += CHARS.charAt(((c1 & 0x3) << 4) | ((c2 & 0xF0) >> 4));
            out += CHARS.charAt(((c2 & 0xF) << 2) | ((c3 & 0xC0) >> 6));
            out += CHARS.charAt(c3 & 0x3F);
        }
        return out;
    }
    return LinegaeUtils;
});