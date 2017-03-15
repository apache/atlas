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

define(['require',
    'backbone',
    'hbs!tmpl/graph/LineageLayoutView_tmpl',
    'collection/VLineageList',
    'models/VEntity',
    'utils/Utils',
    'dagreD3',
    'd3-tip',
    'utils/Enums',
    'utils/UrlLinks',
    'jquery-ui'
], function(require, Backbone, LineageLayoutViewtmpl, VLineageList, VEntity, Utils, dagreD3, d3Tip, Enums, UrlLinks) {
    'use strict';

    var LineageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends LineageLayoutView */
        {
            _viewName: 'LineageLayoutView',

            template: LineageLayoutViewtmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                graph: ".graph"
            },

            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },

            /**
             * intialize a new LineageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'guid', 'entityDefCollection'));
                this.entityModel = new VEntity();
                this.collection = new VLineageList();
                this.typeMap = {};
                this.asyncFetchCounter = 0;
                this.fetchGraphData();
            },
            onRender: function() {
                var that = this;
                this.$('.fontLoader').show();
                this.$(".resize-graph").resizable({
                    handles: ' s',
                    minHeight: 355,
                    stop: function(event, ui) {
                        that.$('svg').height(($(this).height() - 5))
                    },
                });
                this.g = new dagreD3.graphlib.Graph()
                    .setGraph({
                        nodesep: 50,
                        ranksep: 90,
                        rankdir: "LR",
                        marginx: 20,
                        marginy: 20,
                        transition: function transition(selection) {
                            return selection.transition().duration(500);
                        }
                    })
                    .setDefaultEdgeLabel(function() {
                        return {};
                    });
            },
            fetchGraphData: function() {
                var that = this;
                this.fromToObj = {};
                this.collection.getLineage(this.guid, {
                    skipDefaultError: true,
                    success: function(data) {
                        if (data.relations.length) {
                            that.generateData(data.relations, data.guidEntityMap);
                        } else {
                            that.noLineage();
                        }
                    },
                    cust_error: function(model, response) {
                        that.noLineage();
                    }
                })
            },
            noLineage: function() {
                this.$('.fontLoader').hide();
                //this.$('svg').height('100');
                this.$('svg').html('<text x="' + (this.$('svg').width() - 150) / 2 + '" y="' + this.$('svg').height() / 2 + '" fill="black">No lineage data found</text>');
            },
            generateData: function(relations, guidEntityMap) {
                var that = this;

                function makeNodeObj(relationObj) {
                    var obj = {};
                    obj['shape'] = "img";
                    obj['typeName'] = relationObj.typeName
                    obj['label'] = relationObj.displayText.trunc(18);
                    obj['toolTipLabel'] = relationObj.displayText;
                    obj['id'] = relationObj.guid;
                    obj['isLineage'] = true;
                    obj['queryText'] = relationObj.queryText;
                    if (relationObj.status) {
                        obj['status'] = relationObj.status;
                    }
                    var entityDef = that.entityDefCollection.fullCollection.find({ name: relationObj.typeName });
                    if (entityDef && entityDef.get('superTypes')) {
                        obj['isProcess'] = _.contains(entityDef.get('superTypes'), "Process") ? true : false;
                    }

                    return obj;
                }

                _.each(relations, function(obj, index) {
                    if (!that.fromToObj[obj.fromEntityId]) {
                        that.fromToObj[obj.fromEntityId] = makeNodeObj(guidEntityMap[obj.fromEntityId]);
                        that.g.setNode(obj.fromEntityId, that.fromToObj[obj.fromEntityId]);
                    }
                    if (!that.fromToObj[obj.toEntityId]) {
                        that.fromToObj[obj.toEntityId] = makeNodeObj(guidEntityMap[obj.toEntityId]);
                        that.g.setNode(obj.toEntityId, that.fromToObj[obj.toEntityId]);
                    }
                    var styleObj = {
                        fill: 'none',
                        stroke: '#8bc152'
                    }
                    that.g.setEdge(obj.fromEntityId, obj.toEntityId, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis', "style": "fill:" + styleObj.fill + ";stroke:" + styleObj.stroke + "", 'styleObj': styleObj });
                });

                if (this.fromToObj[this.guid]) {
                    this.fromToObj[this.guid]['isLineage'] = false;
                    this.checkForLineageOrImpactFlag(relations, this.guid);
                }
                if (this.asyncFetchCounter == 0) {
                    this.createGraph();
                }
            },
            checkForLineageOrImpactFlag: function(relations, guid) {
                var that = this,
                    nodeFound = _.where(relations, { 'fromEntityId': guid });
                if (nodeFound.length) {
                    _.each(nodeFound, function(node) {
                        that.fromToObj[node.toEntityId]['isLineage'] = false;
                        var styleObj = {
                            fill: 'none',
                            stroke: '#fb4200'
                        }
                        that.g.setEdge(node.fromEntityId, node.toEntityId, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis', "style": "fill:" + styleObj.fill + ";stroke:" + styleObj.stroke + "", 'styleObj': styleObj });
                        that.checkForLineageOrImpactFlag(relations, node.toEntityId);
                    });
                }
            },
            createGraph: function() {
                var that = this

                this.g.nodes().forEach(function(v) {
                    var node = that.g.node(v);
                    // Round the corners of the nodes
                    if (node) {
                        node.rx = node.ry = 5;
                    }
                });
                // Create the renderer
                var render = new dagreD3.render();
                // Add our custom arrow (a hollow-point)
                render.arrows().arrowPoint = function normal(parent, id, edge, type) {
                    var marker = parent.append("marker")
                        .attr("id", id)
                        .attr("viewBox", "0 0 10 10")
                        .attr("refX", 9)
                        .attr("refY", 5)
                        .attr("markerUnits", "strokeWidth")
                        .attr("markerWidth", 10)
                        .attr("markerHeight", 8)
                        .attr("orient", "auto");

                    var path = marker.append("path")
                        .attr("d", "M 0 0 L 10 5 L 0 10 z")
                        .style("stroke-width", 1)
                        .style("stroke-dasharray", "1,0")
                        .style("fill", edge.styleObj.stroke)
                        .style("stroke", edge.styleObj.stroke);
                    dagreD3.util.applyStyle(path, edge[type + "Style"]);
                };
                render.shapes().img = function circle(parent, bbox, node) {
                    //var r = Math.max(bbox.width, bbox.height) / 2,
                    var shapeSvg = parent.insert("image")
                        .attr("class", "nodeImage")
                        .attr("xlink:href", function(d) {
                            if (node) {
                                if (node.isProcess) {
                                    if (Enums.entityStateReadOnly[node.status]) {
                                        return '../img/icon-gear-delete.png';
                                    } else if (node.id == that.guid) {
                                        return '../img/icon-gear-active.png';
                                    } else {
                                        return '../img/icon-gear.png';
                                    }
                                } else {
                                    if (Enums.entityStateReadOnly[node.status]) {
                                        return '../img/icon-table-delete.png';
                                    } else if (node.id == that.guid) {
                                        return '../img/icon-table-active.png';
                                    } else {
                                        return '../img/icon-table.png';
                                    }
                                }
                            }
                        }).attr("x", "-12px")
                        .attr("y", "-12px")
                        .attr("width", "24px")
                        .attr("height", "24px");
                    node.intersect = function(point) {
                        //return dagreD3.intersect.circle(node, points, point);
                        return dagreD3.intersect.circle(node, 13, point);
                    };
                    return shapeSvg;
                };
                // Set up an SVG group so that we can translate the final graph.
                var svg = d3.select(this.$("svg")[0]),
                    svgGroup = svg.append("g");
                var zoom = d3.behavior.zoom()
                    .scaleExtent([0.5, 6])
                    .on("zoom", zoomed);

                function zoomed() {
                    svgGroup.attr("transform",
                        "translate(" + zoom.translate() + ")" +
                        "scale(" + zoom.scale() + ")"
                    );
                }

                function interpolateZoom(translate, scale) {
                    var self = this;
                    return d3.transition().duration(350).tween("zoom", function() {
                        var iTranslate = d3.interpolate(zoom.translate(), translate),
                            iScale = d3.interpolate(zoom.scale(), scale);
                        return function(t) {
                            zoom
                                .scale(iScale(t))
                                .translate(iTranslate(t));
                            zoomed();
                        };
                    });
                }

                function zoomClick() {
                    var clicked = d3.event.target,
                        direction = 1,
                        factor = 0.2,
                        target_zoom = 1,
                        center = [that.g.graph().width / 2, that.g.graph().height / 2],
                        extent = zoom.scaleExtent(),
                        translate = zoom.translate(),
                        translate0 = [],
                        l = [],
                        view = { x: translate[0], y: translate[1], k: zoom.scale() };

                    d3.event.preventDefault();
                    direction = (this.id === 'zoom_in') ? 1 : -1;
                    target_zoom = zoom.scale() * (1 + factor * direction);

                    if (target_zoom < extent[0] || target_zoom > extent[1]) {
                        return false;
                    }

                    translate0 = [(center[0] - view.x) / view.k, (center[1] - view.y) / view.k];
                    view.k = target_zoom;
                    l = [translate0[0] * view.k + view.x, translate0[1] * view.k + view.y];

                    view.x += center[0] - l[0];
                    view.y += center[1] - l[1];

                    interpolateZoom([view.x, view.y], view.k);
                }
                d3.selectAll('button.zoomButton').on('click', zoomClick);
                var tooltip = d3Tip()
                    .attr('class', 'd3-tip')
                    .offset([-13, 0])
                    .html(function(d) {
                        var value = that.g.node(d);
                        var htmlStr = "";
                        if (value.id !== that.guid) {
                            htmlStr = "<h5 style='text-align: center;'>" + (value.isLineage ? "Lineage" : "Impact") + "</h5>";
                        }
                        htmlStr += "<h5 class='text-center'><span style='color:#359f89'>" + value.toolTipLabel + "</span></h5> ";
                        if (value.typeName) {
                            htmlStr += "<h5 class='text-center'><span>(" + value.typeName + ")</span></h5> ";
                        }
                        if (value.queryText) {
                            htmlStr += "<h5>Query: <span style='color:#359f89'>" + value.queryText + "</span></h5> ";
                        }
                        return htmlStr;
                    });
                svg.call(zoom)
                    .call(tooltip);
                this.$('.fontLoader').hide();
                render(svgGroup, this.g);
                svg.on("dblclick.zoom", null)
                    .on("wheel.zoom", null);
                //change text postion 
                svgGroup.selectAll("g.nodes g.label")
                    .attr("transform", "translate(2,-30)");
                svgGroup.selectAll("g.nodes g.node")
                    .on('mouseenter', function(d) {
                        tooltip.show(d);
                    })
                    .on('dblclick', function(d) {
                        tooltip.hide(d);
                        Utils.setUrl({
                            url: '#!/detailPage/' + d,
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    })
                    .on('mouseleave', function(d) {
                        tooltip.hide(d);
                    });
                // Center the graph
                var initialScale = 1.2;
                zoom.translate([(this.$('svg').width() - this.g.graph().width * initialScale) / 2, (this.$('svg').height() - this.g.graph().height * initialScale) / 2])
                    .scale(initialScale)
                    .event(svg);
                //svg.attr('height', this.g.graph().height * initialScale + 40);

            }
        });
    return LineageLayoutView;
});
