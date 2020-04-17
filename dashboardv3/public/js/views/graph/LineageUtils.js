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

define(['require', 'utils/Utils'], function(require, Utils) {
    'use strict';
    var LineageUtils = {};
    LineageUtils.DragNode = function(options) {
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
                LineageUtils.refreshGraphForIE({ "edgeEl": edgePathEl })
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
    LineageUtils.refreshGraphForSafari = function(options) {
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
    LineageUtils.refreshGraphForIE = function(options) {
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
    LineageUtils.centerNode = function(options) {
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
                if (platform.name === "IE") {
                    LineageUtils.refreshGraphForIE({ "edgeEl": edgePathEl })
                }
            }
        }
    }
    LineageUtils.onHoverFade = function(options) {
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
    LineageUtils.base64Encode = function(options) {
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
    LineageUtils.imgShapeRender = function(parent, bbox, node, viewOptions) {
        var LineageUtilsRef = this,
            imageIconPath = Utils.getEntityIconPath({ entityData: node }),
            imgName = imageIconPath.split("/").pop(),
            viewGuid = viewOptions.guid,
            dagreD3 = viewOptions.dagreD3,
            imageObject = viewOptions.imageObject,
            $defs = viewOptions.$defs;
        if (node.isDeleted) {
            imgName = "deleted_" + imgName;
        }
        if (node.id == viewGuid) {
            var currentNode = true
        }
        var shapeSvg = parent.append('circle')
            .attr('fill', 'url(#img_' + imgName + ')')
            .attr('r', '24px')
            .attr('data-stroke', node.id)
            .attr('stroke-width', "2px")
            .attr("class", "nodeImage " + (currentNode ? "currentNode" : (node.isProcess ? "process" : "node")));
        if (currentNode) {
            shapeSvg.attr("stroke", "#fb4200")
        }

        if (node.isIncomplete === true) {
            parent.attr("class", "node isIncomplete show");
            parent.insert("foreignObject")
                .attr("x", "-25")
                .attr("y", "-25")
                .attr("width", "50")
                .attr("height", "50")
                .append("xhtml:div")
                .insert("i")
                .attr("class", "fa fa-hourglass-half");
        }

        if ($defs.select('pattern[id="img_' + imgName + '"]').empty()) {
            var $pattern = $defs.append("pattern")
                .attr("x", "0%")
                .attr("y", "0%")
                .attr("patternUnits", "objectBoundingBox")
                .attr("id", "img_" + imgName)
                .attr("width", "100%")
                .attr("height", "100%")
                .append('image')
                .attr("href", function(d) {
                    var that = this;
                    if (node) {
                        var getImageData = function(options) {
                            var imagePath = options.imagePath,
                                ajaxOptions = {
                                    "url": imagePath,
                                    "method": "get",
                                    "cache": true
                                }

                            if (platform.name !== "IE") {
                                ajaxOptions["mimeType"] = "text/plain; charset=x-user-defined";
                            }
                            shapeSvg.attr("data-iconpath", imagePath);
                            $.ajax(ajaxOptions)
                                .always(function(data, status, xhr) {
                                    if (data.status == 404) {
                                        getImageData({
                                            "imagePath": Utils.getEntityIconPath({ entityData: node, errorUrl: imagePath })
                                        });
                                    } else if (data) {
                                        if (platform.name !== "IE") {
                                            imageObject[imageIconPath] = 'data:image/png;base64,' + LineageUtilsRef.base64Encode({ "data": data });
                                        } else {
                                            imageObject[imageIconPath] = imagePath;
                                        }
                                        d3.select(that).attr("xlink:href", imageObject[imageIconPath]);
                                        if (imageIconPath !== shapeSvg.attr("data-iconpath")) {
                                            shapeSvg.attr("data-iconpathorigin", imageIconPath);
                                        }
                                    }
                                });
                        }
                        getImageData({
                            "imagePath": imageIconPath
                        });
                    }
                })
                .attr("x", "4")
                .attr("y", currentNode ? "3" : "4").attr("width", "40")
                .attr("height", "40");

        }

        node.intersect = function(point) {
            return dagreD3.intersect.circle(node, currentNode ? 24 : 21, point);
        };
        return shapeSvg;
    }
    LineageUtils.arrowPointRender = function(parent, id, edge, type, viewOptions) {
        var node = parent.node(),
            parentNode = node ? node.parentNode : parent,
            dagreD3 = viewOptions.dagreD3;
        d3.select(parentNode).select('path.path').attr('marker-end', "url(#" + id + ")");
        var marker = parent.append("marker")
            .attr("id", id)
            .attr("viewBox", "0 0 10 10")
            .attr("refX", 8)
            .attr("refY", 5)
            .attr("markerUnits", "strokeWidth")
            .attr("markerWidth", 4)
            .attr("markerHeight", 4)
            .attr("orient", "auto");

        var path = marker.append("path")
            .attr("d", "M 0 0 L 10 5 L 0 10 z")
            .style("fill", edge.styleObj.stroke);
        dagreD3.util.applyStyle(path, edge[type + "Style"]);
    }
    LineageUtils.BezierCurve = function(context) {
        return {
            lineStart: function() {
                this.data = [];
            },
            point: function(x, y) {
                this.data.push([x, y]);
            },
            lineEnd: function() {
                var x0 = this.data[0][0],
                    y0 = this.data[0][1],
                    cp1x = this.data[1][0],
                    cp1y = this.data[1][1],
                    cp2Obj = this.data[this.data.length - 2],
                    cp2x = cp2Obj[0],
                    cp2y = cp2Obj[1],
                    axisObj = this.data[this.data.length - 1],
                    x1 = axisObj[0],
                    y1 = axisObj[1];
                context.moveTo(x0, y0);
                context.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, x1, y1);
            }
        }
    }
    LineageUtils.SaveSvg = function(e, viewOptions) {
        var that = this,
            svg = viewOptions.svg,
            svgWidth = viewOptions.svgWidth,
            svgHeight = viewOptions.svgHeight,
            downloadFileName = viewOptions.downloadFileName,
            toggleLoader = viewOptions.toggleLoader,
            svgClone = svg.cloneNode(true),
            scaleFactor = 1;
        setTimeout(function() {
            if (platform.name === "Firefox") {
                svgClone.setAttribute('width', svgWidth);
                svgClone.setAttribute('height', svgHeight);
            }
            $('.hidden-svg').html(svgClone);
            $(svgClone).find('>g').attr("transform", "scale(" + scaleFactor + ")");
            $(svgClone).find("foreignObject").remove();
            var canvasOffset = { x: 150, y: 150 },
                setWidth = (svgClone.getBBox().width + (canvasOffset.x)),
                setHeight = (svgClone.getBBox().height + (canvasOffset.y)),
                xAxis = svgClone.getBBox().x,
                yAxis = svgClone.getBBox().y;
            svgClone.attributes.viewBox.value = xAxis + "," + yAxis + "," + setWidth + "," + setHeight;

            var createCanvas = document.createElement('canvas');
            createCanvas.id = "canvas";
            createCanvas.style.display = 'none';

            var body = $('body').append(createCanvas),
                canvas = $('canvas')[0];
            canvas.width = (svgClone.getBBox().width * scaleFactor) + canvasOffset.x;
            canvas.height = (svgClone.getBBox().height * scaleFactor) + canvasOffset.y;

            var ctx = canvas.getContext('2d'),
                data = (new XMLSerializer()).serializeToString(svgClone),
                DOMURL = window.URL || window.webkitURL || window;

            ctx.fillStyle = "#FFFFFF";
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            ctx.strokeRect(0, 0, canvas.width, canvas.height);
            ctx.restore();

            var img = new Image(canvas.width, canvas.height);
            var svgBlob = new Blob([data], { type: 'image/svg+xml;base64' });
            if (platform.name === "Safari") {
                svgBlob = new Blob([data], { type: 'image/svg+xml' });
            }
            var url = DOMURL.createObjectURL(svgBlob);

            img.onload = function() {
                try {
                    var a = document.createElement("a");
                    a.download = downloadFileName;
                    document.body.appendChild(a);
                    ctx.drawImage(img, 50, 50, canvas.width, canvas.height);
                    canvas.toBlob(function(blob) {
                        if (!blob) {
                            Utils.notifyError({
                                content: "There was an error in downloading Lineage!"
                            });
                            toggleLoader();
                            return;
                        }
                        a.href = DOMURL.createObjectURL(blob);
                        if (blob.size > 10000000) {
                            Utils.notifyWarn({
                                content: "The Image size is huge, please open the image in a browser!"
                            });
                        }
                        a.click();
                        toggleLoader();
                        if (platform.name === 'Safari') {
                            that.refreshGraphForSafari({
                                edgeEl: that.$('svg g.node')
                            });
                        }
                    }, 'image/png');
                    $('.hidden-svg').html('');
                    createCanvas.remove();

                } catch (err) {
                    Utils.notifyError({
                        content: "There was an error in downloading Lineage!"
                    });
                    toggleLoader();
                }

            };
            img.src = url;
        }, 0);
    }
    return LineageUtils;
});