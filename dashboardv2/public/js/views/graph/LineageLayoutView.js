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
    'views/graph/LineageUtils',
    'dagreD3',
    'd3-tip',
    'utils/Enums',
    'utils/UrlLinks',
    'utils/Globals',
    'utils/CommonViewFunction',
    'platform',
    'jquery-ui'
], function(require, Backbone, LineageLayoutViewtmpl, VLineageList, VEntity, Utils, LineageUtils, dagreD3, d3Tip, Enums, UrlLinks, Globals, CommonViewFunction, platform) {
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
                filterToggler: '[data-id="filter-toggler"]',
                settingToggler: '[data-id="setting-toggler"]',
                searchToggler: '[data-id="search-toggler"]',
                boxClose: '[data-id="box-close"]',
                lineageFullscreenToggler: '[data-id="fullScreen-toggler"]',
                filterBox: '.filter-box',
                searchBox: '.search-box',
                settingBox: '.setting-box',
                lineageTypeSearch: '[data-id="typeSearch"]',
                searchNode: '[data-id="searchNode"]',
                nodeDetailTable: '[data-id="nodeDetailTable"]',
                showOnlyHoverPath: '[data-id="showOnlyHoverPath"]',
                showTooltip: '[data-id="showTooltip"]',
                saveSvg: '[data-id="saveSvg"]',
                resetLineage: '[data-id="resetLineage"]'
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
                events["click " + this.ui.filterToggler] = 'onClickFilterToggler';
                events["click " + this.ui.boxClose] = 'toggleBoxPanel';
                events["click " + this.ui.settingToggler] = 'onClickSettingToggler';
                events["click " + this.ui.lineageFullscreenToggler] = 'onClickLineageFullscreenToggler';
                events["click " + this.ui.searchToggler] = 'onClickSearchToggler';
                events["click " + this.ui.saveSvg] = 'onClickSaveSvg';
                events["click " + this.ui.resetLineage] = 'onClickResetLineage';
                return events;
            },

            /**
             * intialize a new LineageLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'processCheck', 'guid', 'entity', 'entityName', 'entityDefCollection', 'actionCallBack', 'fetchCollection', 'attributeDefs'));
                this.collection = new VLineageList();
                this.lineageData = null;
                this.typeMap = {};
                this.apiGuid = {};
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

                if (platform.name === "Microsoft Edge" || platform.name === "IE") {
                    $(that.ui.saveSvg).hide();

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
                if (icon.hasClass('fa-expand')) {
                    icon.parent('button').attr("data-original-title", "Full Screen");
                } else {
                    icon.parent('button').attr("data-original-title", "Default View");
                }
                panel.toggleClass('fullscreen-mode');
            },
            onCheckUnwantedEntity: function(e) {
                var data = $.extend(true, {}, this.lineageData);
                //this.fromToNodeData = {};
                this.initializeGraph();
                if ($(e.target).data("id") === "checkHideProcess") {
                    this.filterObj.isProcessHideCheck = e.target.checked;
                } else {
                    this.filterObj.isDeletedEntityHideCheck = e.target.checked;
                }
                this.generateData({ "relationshipMap": this.relationshipMap, "guidEntityMap": this.guidEntityMap });
            },
            toggleBoxPanel: function(options) {
                var el = options && options.el,
                    nodeDetailToggler = options && options.nodeDetailToggler,
                    currentTarget = options.currentTarget;
                this.$el.find('.show-box-panel').removeClass('show-box-panel');
                if (el && el.addClass) {
                    el.addClass('show-box-panel');
                }
                this.$('circle.node-detail-highlight').removeClass("node-detail-highlight");
            },
            onClickNodeToggler: function(options) {
                this.toggleBoxPanel({ el: this.$('.lineage-node-detail'), nodeDetailToggler: true });
            },
            onClickFilterToggler: function() {
                this.toggleBoxPanel({ el: this.ui.filterBox });
            },
            onClickSettingToggler: function() {
                this.toggleBoxPanel({ el: this.ui.settingBox });
            },
            onClickSearchToggler: function() {
                this.toggleBoxPanel({ el: this.ui.searchBox });
            },
            onSelectDepthChange: function(e, options) {
                this.initializeGraph();
                this.filterObj.depthCount = e.currentTarget.value;
                this.fetchGraphData({ queryParam: { 'depth': this.filterObj.depthCount } });
            },

            fetchGraphData: function(options) {
                var that = this,
                    queryParam = options && options.queryParam || {};
                this.fromToNodeData = {};
                this.$('.fontLoader').show();
                this.$('svg>g').hide();
                this.toggleDisableState({
                    "el": that.$(".graph-button-group button,select[data-id='selectDepth']")
                });
                this.collection.getLineage(this.guid, {
                    skipDefaultError: true,
                    queryParam: queryParam,
                    success: function(data) {
                        if (data.relations.length) {
                            that.lineageData = $.extend(true, {}, data);
                            that.relationshipMap = that.crateLineageRelationshipHashMap(data);
                            that.guidEntityMap = $.extend(true, {}, data.guidEntityMap);
                            that.generateData({ "relationshipMap": that.relationshipMap, "guidEntityMap": that.guidEntityMap });
                            that.toggleDisableState({
                                "el": that.$(".graph-button-group button,select[data-id='selectDepth']")
                            });
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
                this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No lineage data found</text>');
                if (this.actionCallBack) {
                    this.actionCallBack();
                }
            },
            hideCheckForProcess: function() {
                this.$('.hideProcessContainer').hide();
            },
            isProcess: function(node) {
                var typeName = node.typeName,
                    superTypes = node.superTypes,
                    entityDef = node.entityDef;
                if (typeName == "Process") {
                    return true;
                }
                return _.contains(superTypes, "Process");
            },
            isDeleted: function(node) {
                if (_.isUndefined(node)) {
                    return
                }
                return Enums.entityStateReadOnly[node.status];
            },
            isNodeToBeUpdated: function(node) {
                var isProcessHideCheck = this.filterObj.isProcessHideCheck,
                    isDeletedEntityHideCheck = this.filterObj.isDeletedEntityHideCheck
                var returnObj = {
                    isProcess: (isProcessHideCheck && node.isProcess),
                    isDeleted: (isDeletedEntityHideCheck && node.isDeleted)

                };
                returnObj["update"] = returnObj.isProcess || returnObj.isDeleted;
                return returnObj;
            },
            getNestedSuperTypes: function(options) {
                var entityDef = options.entityDef;
                return Utils.getNestedSuperTypes({ data: entityDef, collection: this.entityDefCollection })
            },
            getEntityDef: function(typeName) {
                var entityDef = null;
                if (typeName) {
                    entityDef = this.entityDefCollection.fullCollection.find({ name: typeName });
                    entityDef = entityDef ? entityDef.toJSON() : entityDef;
                }
                return entityDef;
            },
            getServiceType: function(options) {
                if (!options) {
                    return;
                }
                var typeName = options.typeName,
                    entityDef = options.entityDef,
                    serviceType = null;
                if (typeName) {
                    if (entityDef) {
                        serviceType = entityDef.serviceType || null;
                    }
                }
                return serviceType;
            },
            crateLineageRelationshipHashMap: function(data) {
                var that = this,
                    relations = data && data.relations,
                    guidEntityMap = data && data.guidEntityMap,
                    makeNodeData = function(relationObj) {
                        var obj = $.extend(true, {
                            shape: "img",
                            label: relationObj.displayText.trunc(18),
                            toolTipLabel: relationObj.displayText,
                            id: relationObj.guid,
                            isLineage: true,
                            entityDef: this.getEntityDef(relationObj.typeName)
                        }, relationObj);
                        obj["serviceType"] = this.getServiceType({ typeName: relationObj.typeName, entityDef: obj.entityDef });
                        obj["superTypes"] = this.getNestedSuperTypes({ entityDef: obj.entityDef });
                        obj['isProcess'] = this.isProcess(obj);
                        obj['isDeleted'] = this.isDeleted(obj);
                        return obj;
                    }.bind(this),
                    newHashMap = {};
                _.each(relations, function(obj) {
                    if (!that.fromToNodeData[obj.fromEntityId]) {
                        that.fromToNodeData[obj.fromEntityId] = makeNodeData(guidEntityMap[obj.fromEntityId]);
                    }
                    if (!that.fromToNodeData[obj.toEntityId]) {
                        that.fromToNodeData[obj.toEntityId] = makeNodeData(guidEntityMap[obj.toEntityId]);
                    }
                    if (newHashMap[obj.fromEntityId]) {
                        newHashMap[obj.fromEntityId].push(obj.toEntityId);
                    } else {
                        newHashMap[obj.fromEntityId] = [obj.toEntityId];
                    }
                });
                return newHashMap;
            },
            generateData: function(options) {
                var that = this,
                    relationshipMap = options && $.extend(true, {}, options.relationshipMap) || {},
                    guidEntityMap = options && options.guidEntityMap || {},
                    styleObj = {
                        fill: 'none',
                        stroke: '#ffb203',
                        width: 3
                    },
                    getStyleObjStr = function(styleObj) {
                        return 'fill:' + styleObj.fill + ';stroke:' + styleObj.stroke + ';stroke-width:' + styleObj.width;
                    },
                    filterRelationshipMap = relationshipMap,
                    isHideFilterOn = this.filterObj.isProcessHideCheck || this.filterObj.isDeletedEntityHideCheck,
                    getNewToNodeRelationship = function(toNodeGuid) {
                        if (toNodeGuid && relationshipMap[toNodeGuid]) {
                            var newRelationship = [];
                            _.each(relationshipMap[toNodeGuid], function(guid) {
                                var nodeToBeUpdated = that.isNodeToBeUpdated(that.fromToNodeData[guid]);
                                if (nodeToBeUpdated.update) {
                                    var newRelation = getNewToNodeRelationship(guid);
                                    if (newRelation) {
                                        newRelationship = newRelationship.concat(newRelation);
                                    }
                                } else {
                                    newRelationship.push(guid);
                                }
                            });
                            return newRelationship;
                        } else {
                            return null;
                        }
                    },
                    getToNodeRelation = function(toNodes, fromNodeToBeUpdated) {
                        var toNodeRelationship = [];
                        _.each(toNodes, function(toNodeGuid) {
                            var toNodeToBeUpdated = that.isNodeToBeUpdated(that.fromToNodeData[toNodeGuid]);
                            if (toNodeToBeUpdated.update) {
                                // To node need to updated
                                if (pendingFromRelationship[toNodeGuid]) {
                                    toNodeRelationship = toNodeRelationship.concat(pendingFromRelationship[toNodeGuid]);
                                } else {
                                    var newToNodeRelationship = getNewToNodeRelationship(toNodeGuid);
                                    if (newToNodeRelationship) {
                                        toNodeRelationship = toNodeRelationship.concat(newToNodeRelationship);
                                    }
                                }
                            } else {
                                //when bothe node not to be updated.
                                toNodeRelationship.push(toNodeGuid);
                            }
                        });
                        return toNodeRelationship;
                    },
                    pendingFromRelationship = {};
                if (isHideFilterOn) {
                    filterRelationshipMap = {};
                    _.each(relationshipMap, function(toNodes, fromNodeGuid) {
                        var fromNodeToBeUpdated = that.isNodeToBeUpdated(that.fromToNodeData[fromNodeGuid]),
                            toNodeList = getToNodeRelation(toNodes, fromNodeToBeUpdated);
                        if (fromNodeToBeUpdated.update) {
                            if (pendingFromRelationship[fromNodeGuid]) {
                                pendingFromRelationship[fromNodeGuid] = pendingFromRelationship[fromNodeGuid].concat(toNodeList);
                            } else {
                                pendingFromRelationship[fromNodeGuid] = toNodeList;
                            }
                        } else {
                            if (filterRelationshipMap[fromNodeGuid]) {
                                filterRelationshipMap[fromNodeGuid] = filterRelationshipMap[fromNodeGuid].concat(toNodeList);
                            } else {
                                filterRelationshipMap[fromNodeGuid] = toNodeList;
                            }
                        }
                    })
                }

                _.each(filterRelationshipMap, function(toNodesList, fromNodeGuid) {
                    if (!that.g._nodes[fromNodeGuid]) {
                        that.g.setNode(fromNodeGuid, that.fromToNodeData[fromNodeGuid]);
                    }
                    _.each(toNodesList, function(toNodeGuid) {
                        if (!that.g._nodes[toNodeGuid]) {
                            that.g.setNode(toNodeGuid, that.fromToNodeData[toNodeGuid]);
                        }
                        that.g.setEdge(fromNodeGuid, toNodeGuid, {
                            "arrowhead": 'arrowPoint',
                            "lineInterpolate": 'basis',
                            "style": getStyleObjStr(styleObj),
                            'styleObj': styleObj
                        });
                    })
                })

                //if no relations found
                if (_.isEmpty(filterRelationshipMap)) {
                    this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No relations to display</text>');
                }

                if (this.fromToNodeData[this.guid]) {
                    this.fromToNodeData[this.guid]['isLineage'] = false;
                    this.findImpactNodeAndUpdateData({ "relationshipMap": filterRelationshipMap, "guid": this.guid, "getStyleObjStr": getStyleObjStr });
                }
                this.renderLineageTypeSearch();
                this.createGraph();
            },
            findImpactNodeAndUpdateData: function(options) {
                var that = this,
                    relationshipMap = options.relationshipMap,
                    fromNodeGuid = options.guid,
                    getStyleObjStr = options.getStyleObjStr,
                    toNodeList = relationshipMap[fromNodeGuid];
                if (toNodeList && toNodeList.length) {
                    if (!relationshipMap[fromNodeGuid]["traversed"]) {
                        relationshipMap[fromNodeGuid]["traversed"] = true;
                        _.each(toNodeList, function(toNodeGuid) {
                            that.fromToNodeData[toNodeGuid]['isLineage'] = false;
                            var styleObj = {
                                fill: 'none',
                                stroke: '#fb4200',
                                width: 3
                            }
                            that.g.setEdge(fromNodeGuid, toNodeGuid, {
                                "arrowhead": 'arrowPoint',
                                "lineInterpolate": 'basis',
                                "style": getStyleObjStr(styleObj),
                                'styleObj': styleObj
                            });
                            that.findImpactNodeAndUpdateData({
                                "relationshipMap": relationshipMap,
                                "guid": toNodeGuid,
                                "getStyleObjStr": getStyleObjStr
                            });
                        });
                    }
                }
            },
            zoomed: function(that) {
                this.$('svg').find('>g').attr("transform",
                    "translate(" + this.zoom.translate() + ")" +
                    "scale(" + this.zoom.scale() + ")"
                );
                LineageUtils.refreshGraphForIE({
                    edgeEl: this.$('svg .edgePath')
                });
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
                    height = this.$('svg').height(),
                    imageObject = {};
                $('.resizeGraph').css("height", height + "px");

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
                    var parentNode = parent && parent[0] && parent[0][0] && parent[0][0].parentNode ? parent[0][0].parentNode : parent;
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
                        .attr('stroke-width', "2px")
                        .attr("class", "nodeImage " + (currentNode ? "currentNode" : (node.isProcess ? "process" : "node")));
                    if (currentNode) {
                        shapeSvg.attr("stroke", "#fb4200")
                    }
                    parent.insert("defs")
                        .append("pattern")
                        .attr("x", "0%")
                        .attr("y", "0%")
                        .attr("patternUnits", "objectBoundingBox")
                        .attr("id", "img_" + node.id)
                        .attr("width", "100%")
                        .attr("height", "100%")
                        .append('image')
                        .attr("href", function(d) {
                            var that = this;
                            if (node) {
                                // to check for IE-10
                                var originLink = window.location.origin;
                                if (platform.name === "IE") {
                                    originLink = window.location.protocol + "//" + window.location.host;
                                }
                                var imageIconPath = Utils.getEntityIconPath({ entityData: node }),
                                    imagePath = ((originLink + Utils.getBaseUrl(window.location.pathname)) + imageIconPath);

                                var getImageData = function(options) {
                                    var imagePath = options.imagePath,
                                        ajaxOptions = {
                                            "url": imagePath,
                                            "method": "get",
                                            "async": false,
                                        }

                                    if (platform.name !== "IE") {
                                        ajaxOptions["mimeType"] = "text/plain; charset=x-user-defined";
                                    }
                                    $.ajax(ajaxOptions)
                                        .always(function(data, status, xhr) {
                                            if (data.status == 404) {
                                                getImageData({
                                                    "imagePath": Utils.getEntityIconPath({ entityData: node, errorUrl: imagePath }),
                                                    "imageIconPath": imageIconPath
                                                });
                                            } else if (data) {
                                                if (platform.name !== "IE") {
                                                    imageObject[imageIconPath] = 'data:image/png;base64,' + LineageUtils.base64Encode({ "data": data });
                                                } else {
                                                    imageObject[imageIconPath] = imagePath;
                                                }
                                            }
                                        });
                                }
                                if (_.keys(imageObject).indexOf(imageIconPath) === -1) {
                                    getImageData({
                                        "imagePath": imagePath,
                                        "imageIconPath": imageIconPath
                                    });
                                }

                                if (_.isUndefined(imageObject[imageIconPath])) {
                                    // before img success
                                    imageObject[imageIconPath] = [d3.select(that)];
                                } else if (_.isArray(imageObject[imageIconPath])) {
                                    // before img success
                                    imageObject[imageIconPath].push(d3.select(that));
                                } else {
                                    d3.select(that).attr("xlink:href", imageObject[imageIconPath]);
                                    return imageObject[imageIconPath];
                                }
                            }
                        })
                        .attr("x", "4")
                        .attr("y", currentNode ? "3" : "4").attr("width", "40")
                        .attr("height", "40");

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
                var waitForDoubleClick = null;
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
                        if (that.ui.showTooltip.prop('checked')) {
                            tooltip.direction(direction).show(d);
                        }

                        if (!that.ui.showOnlyHoverPath.prop('checked')) {
                            return;
                        }
                        that.$('svg').addClass('hover');
                        var nextNode = that.g.successors(d),
                            previousNode = that.g.predecessors(d),
                            nodesToHighlight = nextNode.concat(previousNode);
                        LineageUtils.onHoverFade({
                            opacity: 0.3,
                            selectedNode: d,
                            highlight: nodesToHighlight,
                            svg: that.svg
                        }).init();
                    })
                    .on('mouseleave', function(d) {
                        that.activeNode = false;
                        var nodeEL = this;
                        setTimeout(function(argument) {
                            if (!(that.activeTip || that.activeNode)) {
                                $(nodeEL).removeClass('active');
                                if (that.ui.showTooltip.prop('checked')) {
                                    tooltip.hide(d);
                                }
                            }
                        }, 150);
                        if (!that.ui.showOnlyHoverPath.prop('checked')) {
                            return;
                        }
                        that.$('svg').removeClass('hover');
                        that.$('svg').removeClass('hover-active');
                        LineageUtils.onHoverFade({
                            opacity: 1,
                            selectedNode: d,
                            svg: that.svg
                        }).init();
                    })
                    .on('click', function(d) {
                        var el = this;
                        if (d3.event.defaultPrevented) return; // ignore drag
                        d3.event.preventDefault();

                        if (waitForDoubleClick != null) {
                            clearTimeout(waitForDoubleClick)
                            waitForDoubleClick = null;
                            tooltip.hide(d);
                            Utils.setUrl({
                                url: '#!/detailPage/' + d + '?tabActive=lineage',
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        } else {
                            var currentEvent = d3.event
                            waitForDoubleClick = setTimeout(function() {
                                tooltip.hide(d);
                                that.onClickNodeToggler({ obj: d });
                                $(el).find('circle').addClass('node-detail-highlight');
                                that.updateRelationshipDetails({ guid: d });
                                waitForDoubleClick = null;
                            }, 150)
                        }
                    });

                svgGroup.selectAll("g.edgePath path.path").on('click', function(d) {
                    var data = { obj: _.find(that.lineageData.relations, { "fromEntityId": d.v, "toEntityId": d.w }) };
                    if (data.obj) {
                        var relationshipId = data.obj.relationshipId;
                        require(['views/graph/PropagationPropertyModal'], function(PropagationPropertyModal) {
                            var view = new PropagationPropertyModal({
                                edgeInfo: data,
                                relationshipId: relationshipId,
                                lineageData: that.lineageData,
                                apiGuid: that.apiGuid,
                                detailPageFetchCollection: that.fetchCollection
                            });
                        });
                    }
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
                LineageUtils.centerNode({
                    guid: that.guid,
                    svg: that.$('svg'),
                    g: this.g,
                    edgeEl: $('svg .edgePath'),
                    afterCenterZoomed: function(options) {
                        var newScale = options.newScale,
                            newTranslate = options.newTranslate;
                        that.zoom.scale(newScale);
                        that.zoom.translate(newTranslate);
                    }
                }).init();
                zoom.event(svg);
                if (platform.name === "IE") {
                    LineageUtils.refreshGraphForIE({
                        edgeEl: this.$('svg .edgePath')
                    });
                }
                LineageUtils.DragNode({
                    svg: this.svg,
                    g: this.g,
                    guid: this.guid,
                    edgeEl: this.$('svg .edgePath')
                }).init();
            },
            renderLineageTypeSearch: function() {
                var that = this,
                    lineageData = $.extend(true, {}, this.lineageData),
                    data = [],
                    typeStr = '<option></option>';
                if (!_.isEmpty(lineageData)) {
                    _.each(lineageData.guidEntityMap, function(obj, index) {
                        var nodeData = that.fromToNodeData[obj.guid];
                        if (that.filterObj.isProcessHideCheck && nodeData && nodeData.isProcess) {
                            return;
                        } else if (that.filterObj.isDeletedEntityHideCheck && nodeData && nodeData.isDeleted) {
                            return
                        }
                        typeStr += '<option value="' + obj.guid + '">' + obj.attributes.name + '</option>';
                    });
                }
                that.ui.lineageTypeSearch.html(typeStr);
                this.initilizelineageTypeSearch();
            },
            initilizelineageTypeSearch: function() {
                var that = this;
                that.ui.lineageTypeSearch.select2({
                    closeOnSelect: true,
                    placeholder: 'Select Node'
                }).on('change.select2', function(e) {
                    e.stopPropagation();
                    e.stopImmediatePropagation();
                    d3.selectAll(".serach-rect").remove();
                    var selectedNode = $('[data-id="typeSearch"]').val();
                    that.searchNodeObj.selectedNode = selectedNode;
                    LineageUtils.centerNode({
                        guid: selectedNode,
                        svg: $(that.svg[0]),
                        g: that.g,
                        edgeEl: $('svg .edgePath'),
                        afterCenterZoomed: function(options) {
                            var newScale = options.newScale,
                                newTranslate = options.newTranslate;
                            that.zoom.scale(newScale);
                            that.zoom.translate(newTranslate);
                        }
                    }).init();
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
            updateRelationshipDetails: function(options) {
                var that = this,
                    guid = options.guid,
                    initialData = that.guidEntityMap[guid],
                    typeName = initialData.typeName || guid,
                    attributeDefs = that.g._nodes[guid] && that.g._nodes[guid].entityDef ? that.g._nodes[guid].entityDef.attributeDefs : null;
                this.$("[data-id='typeName']").text(typeName);
                this.entityModel = new VEntity({});
                var config = {
                    guid: 'guid',
                    typeName: 'typeName',
                    name: 'name',
                    qualifiedName: 'qualifiedName',
                    owner: 'owner',
                    createTime: 'createTime',
                    status: 'status',
                    classificationNames: 'classifications',
                    meanings: 'term'
                };
                var data = {};
                _.each(config, function(valKey, key) {
                    var val = initialData[key];
                    if (_.isUndefined(val) && initialData.attributes[key]) {
                        val = initialData.attributes[key];
                    }
                    if (val) {
                        data[valKey] = val;
                    }
                });
                this.ui.nodeDetailTable.html(CommonViewFunction.propertyTable({
                    "scope": this,
                    "valueObject": data,
                    "attributeDefs": attributeDefs,
                    "sortBy": false
                }));
            },
            onClickSaveSvg: function(e, a) {
                var that = this;
                var loaderTargetDiv = $(e.currentTarget).find('>i');

                if (loaderTargetDiv.hasClass('fa-refresh')) {
                    Utils.notifyWarn({
                        content: "Please wait while the lineage gets downloaded"
                    });
                    return false; // return if the lineage is not loaded.
                }


                that.toggleLoader(loaderTargetDiv);
                Utils.notifyInfo({
                    content: "Lineage will be downloaded in a moment."
                });
                setTimeout(function() {
                    var svg = that.$('svg')[0],
                        svgClone = svg.cloneNode(true),
                        scaleFactor = 1,
                        svgWidth = that.$('svg').width(),
                        svgHeight = that.$('svg').height();
                    if (platform.name === "Firefox") {
                        svgClone.setAttribute('width', svgWidth);
                        svgClone.setAttribute('height', svgHeight);
                    }
                    $('.hidden-svg').html(svgClone);
                    $(svgClone).find('>g').attr("transform", "scale(" + scaleFactor + ")");
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
                            var a = document.createElement("a"),
                                entityAttributes = that.entity && that.entity.attributes;
                            a.download = ((entityAttributes && entityAttributes.qualifiedName) || "lineage_export") + ".png";
                            document.body.appendChild(a);
                            ctx.drawImage(img, 50, 50, canvas.width, canvas.height);
                            canvas.toBlob(function(blob) {
                                if (!blob) {
                                    Utils.notifyError({
                                        content: "There was an error in downloading Lineage!"
                                    });
                                    that.toggleLoader(loaderTargetDiv);
                                    return;
                                }
                                a.href = DOMURL.createObjectURL(blob);
                                if (blob.size > 10000000) {
                                    Utils.notifyWarn({
                                        content: "The Image size is huge, please open the image in a browser!"
                                    });
                                }
                                a.click();
                                that.toggleLoader(loaderTargetDiv);
                                if (platform.name === 'Safari') {
                                    LineageUtils.refreshGraphForSafari({
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
                            that.toggleLoader(loaderTargetDiv);
                        }

                    };
                    img.src = url;

                }, 0)
            },
            toggleLoader: function(element) {
                if ((element).hasClass('fa-camera')) {
                    (element).removeClass('fa-camera').addClass("fa-spin-custom fa-refresh");
                } else {
                    (element).removeClass("fa-spin-custom fa-refresh").addClass('fa-camera');
                }
            },
            onClickResetLineage: function() {
                this.createGraph()
            },
            toggleDisableState: function(options) {
                var el = options.el;
                if (el && el.prop) {
                    el.prop("disabled", !el.prop("disabled"));
                }
            }
        });
    return LineageLayoutView;
});