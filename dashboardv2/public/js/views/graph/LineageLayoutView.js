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
    'utils/Globals',
    'platform',
    'jquery-ui'
], function(require, Backbone, LineageLayoutViewtmpl, VLineageList, VEntity, Utils, dagreD3, d3Tip, Enums, UrlLinks, Globals, platform) {
    'use strict';

    var LineageLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends LineageLayoutView */
        {
            _viewName: 'LineageLayoutView',

            template: LineageLayoutViewtmpl,
            className: "resizeGraph",

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                graph: ".graph",
                checkHideProcess: "[data-id='checkHideProcess']",
                checkDeletedEntity: "[data-id='checkDeletedEntity']",
                selectDepth: 'select[data-id="selectDepth"]',
                fltrTogler: '[data-id="fltr-togler"]',
                lineageFilterPanel: '.lineage-fltr-panel',
                LineageFullscreenToggler: '[data-id="fullScreen-toggler"]'
            },

            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.checkHideProcess] = 'onCheckUnwantedEntity';
                events["click " + this.ui.checkDeletedEntity] = 'onCheckUnwantedEntity';
                events['change ' + this.ui.selectDepth] = 'onSelectDepthChange';
                events["click " + this.ui.fltrTogler] = 'onClickFiltrTogler';
                events["click " + this.ui.LineageFullscreenToggler] = 'onClickLineageFullscreenToggler';
                return events;
            },

            /**
             * intialize a new LineageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'processCheck', 'guid', 'entityDefCollection', 'actionCallBack', 'fetchCollection'));
                this.collection = new VLineageList();
                this.lineageData = null;
                this.typeMap = {};
                this.apiGuid = {};
                this.asyncFetchCounter = 0;
                this.edgeCall;
                this.filterObj = {
                    isProcessHideCheck: false,
                    isDeletedEntityHideCheck: false,
                    depthCount: ''
                };
            },

            initializeGraph: function() {
                this.g = {};
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

            onRender: function() {
                var that = this;
                this.fetchGraphData();
                if (platform.name === "IE") {
                    this.$('svg').css('opacity', '0');
                }
                if (this.layoutRendered) {
                    this.layoutRendered();
                }
                if (this.processCheck) {
                    this.hideCheckForProcess();
                }
                this.initializeGraph();
                this.ui.selectDepth.select2({
                    data: _.sortBy([3, 6, 9, 12, 15, 18, 21]),
                    tags: true,
                    dropdownCssClass: "number-input",
                    multiple: false
                });
            },
            onShow: function() {
                this.$('.fontLoader').show();
                this.$el.resizable({
                    handles: ' s',
                    minHeight: 375,
                    stop: function(event, ui) {
                        ui.element.height(($(this).height()));
                    },
                });
            },
            onClickLineageFullscreenToggler: function(e) {
                var icon = $(e.target).find('i'),
                    panel = $(e.target).parents('.tab-pane').first();
                icon.toggleClass('fa-expand fa-compress');
                panel.toggleClass('fullscreen-mode');
            },
            onCheckUnwantedEntity: function(e) {
                var data = $.extend(true, {}, this.lineageData);
                this.fromToObj = {};
                this.initializeGraph();
                if ($(e.target).data("id") === "checkHideProcess") {
                    this.filterObj.isProcessHideCheck = e.target.checked;
                } else {
                    this.filterObj.isDeletedEntityHideCheck = e.target.checked;
                }
                this.generateData(data.relations, data.guidEntityMap);
            },
            onClickFiltrTogler: function() {
                var lineageFilterPanel = this.ui.lineageFilterPanel;
                $(lineageFilterPanel).toggleClass("show-filter-panel");
            },

            onSelectDepthChange: function(e, options) {
                this.initializeGraph();
                this.filterObj.depthCount = e.currentTarget.value;
                this.fetchGraphData({ queryParam: { 'depth': this.filterObj.depthCount } });
            },

            fetchGraphData: function(options) {
                var that = this,
                    queryParam = options && options.queryParam || {};
                this.fromToObj = {};
                this.$('.fontLoader').show();
                this.$('svg>g').hide();
                this.collection.getLineage(this.guid, {
                    skipDefaultError: true,
                    queryParam: queryParam,
                    success: function(data) {
                        if (data.relations.length) {
                            that.lineageData = $.extend(true, {}, data)
                            that.generateData(data.relations, data.guidEntityMap);
                        } else {
                            that.noLineage();
                            that.hideCheckForProcess();
                        }
                    },
                    cust_error: function(model, response) {
                        that.lineageData = [];
                        that.noLineage();
                    },
                    complete: function() {
                        that.$('.fontLoader').hide();
                        that.$('svg>g').show();
                    }
                })
            },
            noLineage: function() {
                this.$('.fontLoader').hide();
                this.$('.depth-container').hide();
                //this.$('svg').height('100');
                this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No lineage data found</text>');
                if (this.actionCallBack) {
                    this.actionCallBack();
                }
            },
            hideCheckForProcess: function() {
                this.$('.hideProcessContainer').hide();
            },
            generateData: function(relations, guidEntityMap) {
                var that = this;

                function isProcess(typeName) {
                    if (typeName == "Process") {
                        return true;
                    }
                    var entityDef = that.entityDefCollection.fullCollection.find({ name: typeName });
                    return _.contains(Utils.getNestedSuperTypes({ data: entityDef.toJSON(), collection: that.entityDefCollection }), "Process")
                }

                function isDeleted(status) {
                    return Enums.entityStateReadOnly[status];
                }

                function isNodeToBeUpdated(node) {
                    if (that.filterObj.isProcessHideCheck) {
                        return isProcess(node.typeName);
                    } else if (that.filterObj.isDeletedEntityHideCheck) {
                        return isDeleted(node.status);
                    }
                }

                function getServiceType(typeName) {
                    var serviceType = null;
                    if (typeName) {
                        var entityDef = that.entityDefCollection.fullCollection.find({ name: typeName });
                        if (entityDef) {
                            serviceType = entityDef.get("serviceType") || null;
                        }
                    }
                    return serviceType;
                }

                function makeNodeObj(relationObj) {
                    var obj = {};
                    obj['shape'] = "img";
                    obj['typeName'] = relationObj.typeName
                    obj['label'] = relationObj.displayText.trunc(18);
                    obj['toolTipLabel'] = relationObj.displayText;
                    obj['id'] = relationObj.guid;
                    obj['isLineage'] = true;
                    obj['queryText'] = relationObj.queryText;
                    obj['serviceType'] = getServiceType(relationObj.typeName);
                    if (relationObj.status) {
                        obj['status'] = relationObj.status;
                    }
                    if (that.filterObj.isProcessHideCheck) {
                        obj['isProcess'] = relationObj.isProcess;
                    } else {
                        obj['isProcess'] = isProcess(relationObj.typeName);
                    }

                    return obj;
                }

                var newRelations = [],
                    finalRelations = relations,
                    isHideFilterOn = this.filterObj.isProcessHideCheck || this.filterObj.isDeletedEntityHideCheck;
                if (isHideFilterOn) {
                    _.each(relations, function(obj, index, relationArr) {
                        var toNodeToBeUpdated = isNodeToBeUpdated(guidEntityMap[obj.toEntityId]);
                        var fromNodeToBeUpdated = isNodeToBeUpdated(guidEntityMap[obj.fromEntityId]);
                        if (toNodeToBeUpdated) {
                            if (that.filterObj.isProcessHideCheck) {
                                //We have already checked entity is process or not inside isNodeToBeUpdated();
                                guidEntityMap[obj.toEntityId]["isProcess"] = true;
                            }
                            _.filter(relationArr, function(flrObj) {
                                if (flrObj.fromEntityId === obj.toEntityId) {
                                    newRelations.push({
                                        fromEntityId: obj.fromEntityId,
                                        toEntityId: flrObj.toEntityId
                                    });
                                }
                            })
                        } else if (fromNodeToBeUpdated) {
                            if (that.filterObj.isProcessHideCheck) {
                                //We have already checked entity is process or not inside isNodeToBeUpdated();
                                guidEntityMap[obj.fromEntityId]["isProcess"] = true;
                            }

                            _.filter(relationArr, function(flrObj) {
                                if (flrObj.toEntityId === obj.fromEntityId) {
                                    newRelations.push({
                                        fromEntityId: flrObj.fromEntityId,
                                        toEntityId: obj.toEntityId
                                    });
                                }
                            })
                        } else {
                            newRelations.push(obj);
                        }
                    });
                    finalRelations = newRelations;
                }

                _.each(finalRelations, function(obj, index) {
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
                        stroke: '#ffb203',
                        width: 3
                    }
                    that.g.setEdge(obj.fromEntityId, obj.toEntityId, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis', "style": "fill:" + styleObj.fill + ";stroke:" + styleObj.stroke + ";stroke-width:" + styleObj.width + "", 'styleObj': styleObj });
                });

                if (this.fromToObj[this.guid]) {
                    this.fromToObj[this.guid]['isLineage'] = false;
                    this.checkForLineageOrImpactFlag(finalRelations, this.guid);
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
                        if (!node["traversed"]) {
                            node["traversed"] = true;
                            that.fromToObj[node.toEntityId]['isLineage'] = false;
                            var styleObj = {
                                fill: 'none',
                                stroke: '#fb4200',
                                width: 3
                            }
                            that.g.setEdge(node.fromEntityId, node.toEntityId, { 'arrowhead': "arrowPoint", lineInterpolate: 'basis', "style": "fill:" + styleObj.fill + ";stroke:" + styleObj.stroke + ";stroke-width:" + styleObj.width + "", 'styleObj': styleObj });
                            that.checkForLineageOrImpactFlag(relations, node.toEntityId);
                        }
                    });
                }
            },
            toggleInformationSlider: function(options) {
                if (options.open && !this.$('.lineage-edge-details').hasClass("open")) {
                    this.$('.lineage-edge-details').addClass('open');
                } else if (options.close && this.$('.lineage-edge-details').hasClass("open")) {
                    d3.selectAll('circle').attr("stroke", "none");
                    this.$('.lineage-edge-details').removeClass('open');
                }
            },
            setGraphZoomPositionCal: function(argument) {
                var initialScale = 1.6,
                    svgEl = this.$('svg'),
                    scaleEl = this.$('svg').find('>g'),
                    translateValue = [(this.$('svg').width() - this.g.graph().width * initialScale) / 2, (this.$('svg').height() - this.g.graph().height * initialScale) / 2]
                if (_.keys(this.g._nodes).length > 15) {
                    initialScale = 0;
                    this.$('svg').addClass('noScale');
                }
                if (svgEl.parents('.panel.panel-fullscreen').length) {
                    translateValue = [20, 20];
                    if (svgEl.hasClass('noScale')) {
                        if (!scaleEl.hasClass('scaleLinage')) {
                            scaleEl.addClass('scaleLinage');
                            initialScale = 1.6;
                        } else {
                            scaleEl.removeClass('scaleLinage');
                            initialScale = 0;
                        }
                    }
                } else {
                    scaleEl.removeClass('scaleLinage');
                }
                this.zoom.translate(translateValue)
                    .scale(initialScale);
            },
            zoomed: function(that) {
                this.$('svg').find('>g').attr("transform",
                    "translate(" + this.zoom.translate() + ")" +
                    "scale(" + this.zoom.scale() + ")"
                );
            },
            createGraph: function() {
                var that = this,
                    width = this.$('svg').width(),
                    height = this.$('svg').height();
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
                };
                render.shapes().img = function circle(parent, bbox, node) {
                    //var r = Math.max(bbox.width, bbox.height) / 2,
                    if (node.id == that.guid) {
                        var currentNode = true
                    }
                    var shapeSvg = parent.append('circle')
                        .attr('fill', 'url(#img_' + node.id + ')')
                        .attr('r', '24px')
                        .attr("class", "nodeImage " + (currentNode ? "currentNode" : (node.isProcess ? "process" : "node")));

                    parent.insert("defs")
                        .append("pattern")
                        .attr("x", "0%")
                        .attr("y", "0%")
                        .attr("patternUnits", "objectBoundingBox")
                        .attr("id", "img_" + node.id)
                        .attr("width", "100%")
                        .attr("height", "100%")
                        .append('image')
                        .attr("xlink:href", function(d) {
                            if (node) {
                                return Utils.getEntityIconPath({ entityData: node });
                            }
                        })
                        .attr("x", currentNode ? "3" : "6")
                        .attr("y", currentNode ? "3" : "4")
                        .attr("width", "40")
                        .attr("height", "40")
                        .on("error", function() {
                            this.setAttributeNS('http://www.w3.org/1999/xlink', 'xlink:href', Utils.getEntityIconPath({ entityData: node, errorUrl: this.href.baseVal }));
                        });

                    node.intersect = function(point) {
                        return dagreD3.intersect.circle(node, currentNode ? 24 : 21, point);
                    };
                    return shapeSvg;
                };
                // Set up an SVG group so that we can translate the final graph.
                if (this.$("svg").find('.output').length) {
                    this.$("svg").find('.output').parent('g').remove();
                }
                var svg = this.svg = d3.select(this.$("svg")[0])
                    .attr("viewBox", "0 0 " + width + " " + height)
                    .attr("enable-background", "new 0 0 " + width + " " + height),
                    svgGroup = svg.append("g");
                var zoom = this.zoom = d3.behavior.zoom()
                    .scaleExtent([0.5, 6])
                    .on("zoom", that.zoomed.bind(this));


                function interpolateZoom(translate, scale) {
                    var self = this;
                    return d3.transition().duration(350).tween("zoom", function() {
                        var iTranslate = d3.interpolate(zoom.translate(), translate),
                            iScale = d3.interpolate(zoom.scale(), scale);
                        return function(t) {
                            zoom
                                .scale(iScale(t))
                                .translate(iTranslate(t));
                            that.zoomed();
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
                d3.selectAll(this.$('.lineageZoomButton')).on('click', zoomClick);
                var tooltip = d3Tip()
                    .attr('class', 'd3-tip')
                    .offset([10, 0])
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
                        return "<div class='tip-inner-scroll'>" + htmlStr + "</div>";
                    });

                svg.call(zoom)
                    .call(tooltip);
                if (platform.name !== "IE") {
                    this.$('.fontLoader').hide();
                }
                render(svgGroup, this.g);
                svg.on("dblclick.zoom", null)
                    .on("wheel.zoom", null);
                //change text postion 
                svgGroup.selectAll("g.nodes g.label")
                    .attr("transform", "translate(2,-35)");
                svgGroup.selectAll("g.nodes g.node")
                    .on('mouseenter', function(d) {
                        that.activeNode = true;
                        var matrix = this.getScreenCTM()
                            .translate(+this.getAttribute("cx"), +this.getAttribute("cy"));
                        that.$('svg').find('.node').removeClass('active');
                        $(this).addClass('active');

                        // Fix
                        var width = $('body').width();
                        var currentELWidth = $(this).offset();
                        var direction = 'e';
                        if (((width - currentELWidth.left) < 330)) {
                            direction = (((width - currentELWidth.left) < 330) && ((currentELWidth.top) < 400)) ? 'sw' : 'w';
                            if (((width - currentELWidth.left) < 330) && ((currentELWidth.top) > 600)) {
                                direction = 'nw';
                            }
                        } else if (((currentELWidth.top) > 600)) {
                            direction = (((width - currentELWidth.left) < 330) && ((currentELWidth.top) > 600)) ? 'nw' : 'n';
                            if ((currentELWidth.left) < 50) {
                                direction = 'ne'
                            }
                        } else if ((currentELWidth.top) < 400) {
                            direction = ((currentELWidth.left) < 50) ? 'se' : 's';
                        }
                        tooltip.direction(direction).show(d)
                    })
                    .on('dblclick', function(d) {
                        tooltip.hide(d);
                        Utils.setUrl({
                            url: '#!/detailPage/' + d + '?tabActive=lineage',
                            mergeBrowserUrl: false,
                            trigger: true
                        });
                    }).on('mouseleave', function(d) {
                        that.activeNode = false;
                        var nodeEL = this;
                        setTimeout(function(argument) {
                            if (!(that.activeTip || that.activeNode)) {
                                $(nodeEL).removeClass('active');
                                tooltip.hide(d);
                            }
                        }, 400)
                    });
                svgGroup.selectAll("g.edgePath path.path").on('click', function(d) {
                    var data = { obj: _.find(that.lineageData.relations, { "fromEntityId": d.v, "toEntityId": d.w }) },
                        relationshipId = data.obj.relationshipId;
                    require(['views/graph/PropagationPropertyModal'], function(PropagationPropertyModal) {
                        var view = new PropagationPropertyModal({
                            edgeInfo: data,
                            relationshipId: relationshipId,
                            lineageData: that.lineageData,
                            apiGuid: that.apiGuid,
                            detailPageFetchCollection: that.fetchCollection
                        });
                    });
                })
                $('body').on('mouseover', '.d3-tip', function(el) {
                    that.activeTip = true;
                });
                $('body').on('mouseleave', '.d3-tip', function(el) {
                    that.activeTip = false;
                    that.$('svg').find('.node').removeClass('active');
                    tooltip.hide();
                });

                // Center the graph
                this.setGraphZoomPositionCal();
                zoom.event(svg);
                //svg.attr('height', this.g.graph().height * initialScale + 40);
                if (platform.name === "IE") {
                    this.IEGraphRenderDone = 0;
                    this.$('svg .edgePath').each(function(argument) {
                        var childNode = $(this).find('marker');
                        $(this).find('marker').remove();
                        var eleRef = this;
                        ++that.IEGraphRenderDone;
                        setTimeout(function(argument) {
                            $(eleRef).find('defs').append(childNode);
                            --that.IEGraphRenderDone;
                            if (that.IEGraphRenderDone === 0) {
                                this.$('.fontLoader').hide();
                                this.$('svg').fadeTo(1000, 1)
                            }
                        }, 1000);
                    });
                }
            }
        });
    return LineageLayoutView;
});