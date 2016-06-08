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
    'd3-tip'
], function(require, Backbone, LineageLayoutViewtmpl, VLineageList, VEntity, Utils, dagreD3, d3Tip) {
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
                _.extend(this, _.pick(options, 'globalVent', 'guid'));
                this.inputCollection = new VLineageList();
                this.outputCollection = new VLineageList();
                this.entityModel = new VEntity();
                this.inputCollection.url = "/api/atlas/lineage/" + this.guid + "/inputs/graph";
                this.outputCollection.url = "/api/atlas/lineage/" + this.guid + "/outputs/graph";
                this.bindEvents();
                this.fetchGraphData();
                this.data = {};
                this.fetchList = 0;
            },
            bindEvents: function() {
                this.listenTo(this.inputCollection, 'reset', function() {
                    this.generateData(this.inputCollection, 'input');
                    this.outputCollection.fetch({ reset: true });
                }, this);
                this.listenTo(this.outputCollection, 'reset', function() {
                    this.generateData(this.outputCollection, 'output');
                    this.outputState = true;
                }, this);
                this.listenTo(this.outputCollection, 'error', function() {
                    this.addNoDataMessage();
                }, this);
                this.listenTo(this.inputCollection, 'error', function() {
                    this.addNoDataMessage();
                }, this);
            },
            onRender: function() {
                this.$('.fontLoader').show();
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
                this.inputCollection.fetch({ reset: true });
            },
            addNoDataMessage: function() {
                //this.$('svg').height('100');
                this.$('svg').html('<text x="' + (this.$('svg').width() - 150) / 2 + '" y="' + this.$('svg').height() / 2 + '" fill="black">No lineage data found</text>');
                this.$('.fontLoader').hide();
            },
            generateData: function(collection, type) {
                var that = this;

                function addValueInObject(data) {
                    var obj = {};
                    if (data && data.definition && data.definition.values) {
                        var values = data.definition.values;
                        obj['label'] = values.name.trunc(20);
                        obj['toolTiplabel'] = values.name;
                        obj['id'] = data.GUID;
                        if (values.queryText) {
                            obj['queryText'] = values.queryText;
                        }
                        obj['shape'] = "img";
                    } else {
                        obj['label'] = vertices[val].values.name;
                        obj['toolTiplabel'] = values.name;
                    }
                    obj['class'] = "type-TOP";
                    if (data.GUID) {
                        that.g.setNode(data.GUID, obj);
                    } else {
                        if (data && data.definition) {
                            if (_.isString(data.definition.id)) {
                                that.g.setNode(data.definition.id, obj);
                            } else if (_.isString(data.definition.id.id)) {
                                that.g.setNode(data.definition.id.id, obj);
                            }
                        }
                    }
                    --that.fetchList;
                    if (that.fetchList <= 0) {
                        if (that.edgesAndvertices) {
                            that.createGraph(that.edgesAndvertices, that.startingPoint);
                        } else if (this.outputState && !that.edgesAndvertices) {
                            that.addNoDataMessage();
                        }
                    }
                }

                function fetchLoadProcess(id) {
                    ++that.fetchList;
                    that.entityModel.getEntity(id, {
                        beforeSend: function() {},
                        success: function(data) {
                            addValueInObject(data);
                        },
                        error: function(error, data, status) {},
                        complete: function() {}
                    });
                }

                function makeNode(c) {
                    var edges = c.edges,
                        vertices = c.vertices,
                        allKeys = [];
                    _.each(c.edges, function(val, key, obj) {
                        allKeys.push(key);
                        _.each(val, function(val1, key1, obj1) {
                            allKeys.push(val1);
                        });
                    });
                    var uniquNode = _.uniq(allKeys);
                    _.each(uniquNode, function(val, key) {
                        var obj = {};
                        if (vertices[val] && vertices[val].values) {
                            obj['label'] = vertices[val].values.name.trunc(20);
                            obj['toolTiplabel'] = vertices[val].values.name;
                            obj['id'] = val;
                            obj['class'] = "type-TOP";
                            obj['shape'] = "img";
                            obj['typeName'] = vertices[val].values.vertexId.values.typeName;
                            if (val && obj) {
                                that.g.setNode(val, obj);
                            }
                        } else {
                            fetchLoadProcess(val);
                        }
                    });
                }
                _.each(collection.models, function(values) {
                    var valuObj = values.get('values');
                    that.startingPoint = [];
                    if (!_.isEmpty(valuObj.edges)) {
                        if (type == "input") {
                            that.edgesAndvertices = {
                                edges: {},
                                vertices: valuObj.vertices
                            };
                            _.each(valuObj.edges, function(val, key, obj) {
                                _.each(val, function(val1, key1, obj1) {
                                    if (!obj[val1]) {
                                        that.startingPoint.push(val1);
                                    }
                                    that.edgesAndvertices.edges[val1] = [key];
                                });
                            });
                        } else {
                            that.edgesAndvertices = valuObj;
                            that.startingPoint = [that.guid];
                        }
                        makeNode(that.edgesAndvertices);
                    } else {
                        if (type == 'output') {
                            that.outputState = true;
                        }
                    }
                });
                if (this.fetchList <= 0) {
                    if (this.edgesAndvertices) {
                        this.createGraph(that.edgesAndvertices, this.startingPoint);
                    } else if (this.outputState && !this.edgesAndvertices) {
                        this.$('.fontLoader').hide();
                        that.$('svg').height('100');
                        that.$('svg').html('<text x="' + (that.$('svg').width() - 150) / 2 + '" y="' + that.$('svg').height() / 2 + '" fill="black">No lineage data found</text>');
                    }
                }
            },
            createGraph: function(edgesAndvertices, startingPoint) {
                var that = this,
                    lastVal = "";
                _.each(startingPoint, function(val, key, obj) {
                    _.each(edgesAndvertices.edges[val], function(val1) {
                        if (val && val1) {
                            that.g.setEdge(val, val1, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis' });
                        }
                        createRemaningEdge(edgesAndvertices.edges, val1);
                    });
                });

                function createRemaningEdge(obj, starting) {
                    if (obj[starting] && obj[starting].length) {
                        _.each(obj[starting], function(val, key) {
                            if (starting && val) {
                                that.g.setEdge(starting, val, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis' });
                            }
                            createRemaningEdge(obj, val);
                        });
                    }
                }

                this.g.nodes().forEach(function(v) {
                    var node = that.g.node(v);
                    // Round the corners of the nodes
                    if (node) {
                        node.rx = node.ry = 5;
                    }
                });
                if (this.outputState) {
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
                            .style("fill", "#cccccc")
                            .style("stroke", "#cccccc");
                        dagreD3.util.applyStyle(path, edge[type + "Style"]);
                    };
                    render.shapes().img = function circle(parent, bbox, node) {
                        //var r = Math.max(bbox.width, bbox.height) / 2,
                        var shapeSvg = parent.insert("image")
                            .attr("class", "nodeImage")
                            .attr("xlink:href", function(d) {
                                if (node) {
                                    if (node.typeName) {
                                        if (node.id == that.guid) {
                                            return '../img/icon-table-active.png';
                                        } else {
                                            return '../img/icon-table.png';
                                        }
                                    } else {
                                        if (node.id == that.guid) {
                                            return '../img/icon-gear-active.png';
                                        } else {
                                            return '../img/icon-gear.png';
                                        }
                                    }
                                }
                            }).attr("x", "-12px")
                            .attr("y", "-12px")
                            .attr("width", "24px")
                            .attr("height", "24px");
                        /*shapeSvg = parent.insert("circle", ":first-child")
                            .attr("x", 35)
                            .attr("y", 35)
                            .attr("r", 20);*/
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
                        .html(function(d) {
                            var value = that.g.node(d);
                            var htmlStr = "<h5>Name: <span style='color:#359f89'>" + value.toolTiplabel + "</span></h5> ";
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

                    svgGroup.selectAll("g.nodes image")
                        .on('mouseover', function(d) {
                            tooltip.show(d);
                        })
                        .on('dblclick', function(d) {
                            tooltip.hide(d);
                            //var urlForTab = window.location.hash.split('/')[1];

                            Utils.setUrl({
                                url: '#!/detailPage/' + d,
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        })
                        .on('mouseout', function(d) {
                            tooltip.hide(d);
                        });
                    // Center the graph
                    var initialScale = 1.2;
                    zoom.translate([(this.$('svg').width() - this.g.graph().width * initialScale) / 2, (this.$('svg').height() - this.g.graph().height * initialScale) / 2])
                        .scale(initialScale)
                        .event(svg);
                    //svg.attr('height', this.g.graph().height * initialScale + 40);
                }
            }
        });
    return LineageLayoutView;
});
