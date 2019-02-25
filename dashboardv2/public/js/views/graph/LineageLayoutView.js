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
                lineageFullscreenToggler: '[data-id="fullScreen-toggler"]',
                searchTogler: '[data-id="search-togler"]',
                lineageSearchPanel: '.lineage-search-panel',
                lineageTypeSearch: '[data-id="typeSearch"]',
                lineageDetailClose: '[data-id="close"]',
                searchNode: '[data-id="searchNode"]',
                nodeEntityList: '[data-id="entityList"]'
            },
            templateHelpers: function() {
                return {
                    width: "100%",
                    height: "100%"
                };
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.checkHideProcess] = 'onCheckUnwantedEntity';
                events["click " + this.ui.checkDeletedEntity] = 'onCheckUnwantedEntity';
                events['change ' + this.ui.selectDepth] = 'onSelectDepthChange';
                events["click " + this.ui.fltrTogler] = 'onClickFiltrTogler';
                events["click " + this.ui.lineageFullscreenToggler] = 'onClickLineageFullscreenToggler';
                events["click " + this.ui.searchTogler] = 'onClickSearchTogler';
                events["click " + this.ui.lineageDetailClose] = function() {
                    this.toggleLineageInfomationSlider({ close: true });
                };
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
                this.searchNodeObj = {
                    selectedNode: ''
                }
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
                var icon = $(e.currentTarget).find('i'),
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
                var lineageSearchPanel = this.ui.lineageSearchPanel;
                $(lineageFilterPanel).toggleClass("show-filter-panel");
                if (lineageSearchPanel.hasClass('show-search-panel')) {
                    $(this.ui.lineageSearchPanel).toggleClass("show-search-panel")
                }
            },
            onClickSearchTogler: function() {
                var lineageSearchPanel = this.ui.lineageSearchPanel;
                var lineageFilterPanel = this.ui.lineageFilterPanel;
                $(lineageSearchPanel).toggleClass("show-search-panel");
                if (lineageFilterPanel.hasClass('show-filter-panel')) {
                    $(this.ui.lineageFilterPanel).toggleClass("show-filter-panel");

                }
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

                function isProcess(node) {
                    if (_.isUndefined(node) || node.typeName == "Process") {
                        return true;
                    }
                    var entityDef = that.entityDefCollection.fullCollection.find({ name: node.typeName });
                    return _.contains(Utils.getNestedSuperTypes({ data: entityDef.toJSON(), collection: that.entityDefCollection }), "Process")
                }

                function isDeleted(node) {
                    if (_.isUndefined(node)) {
                        return
                    }
                    return Enums.entityStateReadOnly[node.status];
                }

                function isNodeToBeUpdated(node) {
                    var isProcessHideCheck = that.filterObj.isProcessHideCheck,
                        isDeletedEntityHideCheck = that.filterObj.isDeletedEntityHideCheck
                    var returnObj = {
                        isProcess: (isProcessHideCheck && isProcess(node)),
                        isDeleted: (isDeletedEntityHideCheck && isDeleted(node))

                    };
                    returnObj["update"] = returnObj.isProcess || returnObj.isDeleted;
                    return returnObj;
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
                        obj['isProcess'] = isProcess(relationObj);
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
                        if (toNodeToBeUpdated.update) {
                            if (that.filterObj.isProcessHideCheck) {
                                //We have already checked entity is process or not inside isNodeToBeUpdated();
                                guidEntityMap[obj.toEntityId]["isProcess"] = true;
                            }
                            _.filter(relationArr, function(flrObj) {
                                if (flrObj.fromEntityId === obj.toEntityId) {
                                    if (that.filterObj.isDeletedEntityHideCheck && isDeleted(guidEntityMap[flrObj.toEntityId])) {
                                        return;
                                    }
                                    newRelations.push({
                                        fromEntityId: obj.fromEntityId,
                                        toEntityId: flrObj.toEntityId
                                    });
                                }
                            })
                        } else if (fromNodeToBeUpdated.update) {
                            if (that.filterObj.isProcessHideCheck) {
                                //We have already checked entity is process or not inside isNodeToBeUpdated();
                                guidEntityMap[obj.fromEntityId]["isProcess"] = true;
                            }

                            _.filter(relationArr, function(flrObj) {
                                if (that.filterObj.isDeletedEntityHideCheck && isDeleted(guidEntityMap[flrObj.fromEntityId])) {
                                    return;
                                }
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
                //if no relations found
                if (!finalRelations.length) {
                    this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No relations to display</text>');
                } else {
                    this.$('svg').html('<text></text>');
                }

                if (this.fromToObj[this.guid]) {
                    this.fromToObj[this.guid]['isLineage'] = false;
                    this.checkForLineageOrImpactFlag(finalRelations, this.guid);
                }
                if (this.asyncFetchCounter == 0) {
                    this.createGraph();
                }
                if (this.lineageData) {
                    this.renderLineageTypeSearch();
                    this.lineageTypeSearch()
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
            centerNode: function(nodeID) {
                var zoom = function() {
                        svgGroup.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
                    },
                    matrix = this.$('svg').find("g.nodes").find('>g#' + nodeID).attr('transform').replace(/[^0-9\-.,]/g, '').split(','),
                    x = matrix[0],
                    y = matrix[1],
                    viewerWidth = this.$('svg').width(),
                    viewerHeight = this.$('svg').height(),
                    gBBox = d3.select('g').node().getBBox(),
                    zoomListener = d3.behavior.zoom().scaleExtent([0.01, 50]).on("zoom", zoom),
                    scale = 1.2,
                    xa = -((x * scale) - (viewerWidth / 2)),
                    ya = -((y * scale) - (viewerHeight / 2));

                this.zoom.translate([xa, ya]);
                d3.select('g').transition()
                    .duration(350)
                    .attr("transform", "translate(" + xa + "," + ya + ")scale(" + scale + ")");
                zoomListener.scale(scale);
                zoomListener.translate([xa, ya]);
                this.zoom.scale(scale);
            },
            zoomed: function(that) {
                this.$('svg').find('>g').attr("transform",
                    "translate(" + this.zoom.translate() + ")" +
                    "scale(" + this.zoom.scale() + ")"
                );
            },
            interpolateZoom: function(translate, scale, that, zoom) {
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
                        .attr('data-stroke', node.id)
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
                    .center([width / 2, height / 2])
                    .scaleExtent([0.01, 50])
                    .on("zoom", that.zoomed.bind(this));

                function zoomClick() {
                    var clicked = d3.event.target,
                        direction = 1,
                        factor = 0.5,
                        target_zoom = 1,
                        center = [width / 2, height / 2],
                        translate = zoom.translate(),
                        translate0 = [],
                        l = [],
                        view = { x: translate[0], y: translate[1], k: zoom.scale() };

                    d3.event.preventDefault();
                    direction = (this.id === 'zoom_in') ? 1 : -1;
                    target_zoom = zoom.scale() * (1 + factor * direction);

                    translate0 = [(center[0] - view.x) / view.k, (center[1] - view.y) / view.k];
                    view.k = target_zoom;
                    l = [translate0[0] * view.k + view.x, translate0[1] * view.k + view.y];

                    view.x += center[0] - l[0];
                    view.y += center[1] - l[1];

                    that.interpolateZoom([view.x, view.y], view.k, that, zoom);
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
                // .on("wheel.zoom", null);
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
                    }).on('click', function(d) {
                        if (d3.event.defaultPrevented) return; // ignore drag
                        that.toggleLineageInfomationSlider({ open: true, obj: d });
                        svgGroup.selectAll('[data-stroke]').attr('stroke', 'none');
                        svgGroup.selectAll('[data-stroke]').attr('stroke', function(c) {
                            if (c == d) {
                                return "#316132";
                            } else {
                                return 'none';
                            }
                        })
                        that.updateRelationshipDetails({ obj: d });

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
                this.centerNode(that.guid);
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
            },
            renderLineageTypeSearch: function() {
                var that = this;
                var lineageData = $.extend(true, {}, this.lineageData);
                var data = [];

                var typeStr = '<option></option>';
                if (!_.isEmpty(lineageData)) {
                    _.each(lineageData.guidEntityMap, function(obj, index) {
                        typeStr += '<option value="' + obj.guid + '">' + obj.attributes.name + '</option>';
                    });
                }
                that.ui.lineageTypeSearch.html(typeStr);
            },
            lineageTypeSearch: function() {
                var that = this;
                that.ui.lineageTypeSearch.select2({
                    closeOnSelect: true,
                    placeholder: 'Select Node'
                }).on('change.select2', function(e) {
                    e.stopPropagation();
                    e.stopImmediatePropagation();
                    d3.selectAll(".serach-rect").remove();
                    var selectedNode = $('[data-id="typeSearch"]').val();
                    that.searchNodeObj.selectedNode = $('[data-id="typeSearch"]').val();
                    that.centerNode(selectedNode);

                    that.svg.selectAll('.nodes g.label').attr('stroke', function(c, d) {
                        if (c == selectedNode) {
                            return "#316132";
                        } else {
                            return 'none';
                        }
                    });
                    // Using jquery for selector because d3 select is not working for few process entities.
                    d3.select($(".node#" + selectedNode)[0]).insert("rect", "circle")
                        .attr("class", "serach-rect")
                        .attr("x", -50)
                        .attr("y", -27.5)
                        .attr("width", 100)
                        .attr("height", 55);
                    d3.selectAll(".nodes circle").classed("wobble", function(d, i, nodes) {
                        if (d == selectedNode) {
                            return true;
                        } else {
                            return false;
                        }
                    });

                });
                if (that.searchNodeObj.selectedNode) {
                    that.ui.lineageTypeSearch.val(that.searchNodeObj.selectedNode);
                    that.ui.lineageTypeSearch.trigger("change.select2");
                }
            },
            toggleLineageInfomationSlider: function(options) {
                if (options.open && !this.$('.lineage-details').hasClass("open")) {
                    this.$('.lineage-details').addClass('open');
                } else if (options.close && this.$('.lineage-details').hasClass("open")) {
                    d3.selectAll('circle').attr("stroke", "none");
                    this.$('.lineage-details').removeClass('open');
                }
            },
            updateRelationshipDetails: function(options) {
                var that = this;
                var lineageData;
                for (var x in that.lineageData.guidEntityMap) {
                    if (x == options.obj) {
                        lineageData = that.lineageData.guidEntityMap[x]
                    }
                }
                var data = lineageData,
                    typeName = data.typeName || options.obj.name,
                    searchString = options.searchString,
                    listString = "";
                this.$("[data-id='typeName']").text(typeName);
                var getElement = function(options) {
                    var showCofig = [
                        "meaningNames",
                        "meanings",
                        "classificationNames",
                        {
                            "attributes": [
                                "description",
                                "name",
                                "qualifiedName"
                            ]
                        }
                    ];
                    var tbody = '';
                    for (var x = 0; x < showCofig.length; x++) {
                        if (typeof showCofig[x] === "object") {
                            _.each(showCofig[x].attributes, function(element, index, list) {
                                var dataToShow = data.attributes[element] ? data.attributes[element] : "N/A";
                                tbody += '<tr><td class="html-cell renderable">' + element + '</td><td  class="html-cell renderable">' + dataToShow + '</td></tr>';
                            })
                        } else {
                            var dataToShow = data[showCofig[x]] ? data[showCofig[x]] : "N/A";
                            dataToShow = dataToShow && dataToShow.length > 0 ? dataToShow : "N/A";
                            tbody += '<tr><td class="html-cell renderable">' + showCofig[x] + '</td><td class="html-cell renderable">' + dataToShow + '</td></tr>';
                        }
                    }
                    var thead = '<thead><tr><th class="renderable">Type</th><th class="renderable">Details</th></thead>';
                    var table = '<table style="word-wrap: break-word;" class="table table-hover ">' + thead + '<tbody>' + tbody + '</body></table>';
                    return table;

                }
                listString += getElement(data);
                this.ui.nodeEntityList.html(listString);
            }
        });
    return LineageLayoutView;
});