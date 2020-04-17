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
                this.ui.searchToggler.prop("disabled", true);
                this.$graphButtonsEl = this.$(".graph-button-group button,select[data-id='selectDepth']")
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
                var that = this;
                this.initializeGraph();
                if ($(e.target).data("id") === "checkHideProcess") {
                    this.filterObj.isProcessHideCheck = e.target.checked;
                } else {
                    this.filterObj.isDeletedEntityHideCheck = e.target.checked;
                }
                that.toggleDisableState({
                    "el": that.$graphButtonsEl
                });
                this.generateData(this.lineageData).then(function() {
                    that.createGraph();
                    that.toggleDisableState({
                        "el": that.$graphButtonsEl
                    });
                });
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
                this.$('.fontLoader').show();
                this.$('svg>g').hide();
                this.toggleDisableState({
                    "el": that.$(".graph-button-group button,select[data-id='selectDepth']")
                });
                this.collection.getLineage(this.guid, {
                    queryParam: queryParam,
                    success: function(data) {
                        if (that.isDestroyed) {
                            return;
                        }
                        if (data.relations.length) {
                            that.lineageData = $.extend(true, {}, data);
                            that.generateData(that.lineageData).then(function(graphObj) {
                                that.createGraph();
                                that.toggleDisableState({
                                    "el": that.$graphButtonsEl
                                });
                            });
                            that.renderLineageTypeSearch().then(function() {
                                that.ui.searchToggler.prop("disabled", false);
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
            generateData: function(options) {
                return new Promise(function(resolve, reject) {
                    try {
                        var that = this,
                            relations = options && options.relations || {},
                            guidEntityMap = options && options.guidEntityMap || {},
                            isHideFilterOn = this.filterObj.isProcessHideCheck || this.filterObj.isDeletedEntityHideCheck,
                            newHashMap = {},
                            styleObj = {
                                fill: 'none',
                                stroke: '#ffb203',
                                width: 3
                            },
                            makeNodeData = function(relationObj) {
                                if (relationObj) {
                                    if (relationObj.updatedValues) {
                                        return relationObj;
                                    }
                                    var obj = _.extend(relationObj, {
                                        shape: "img",
                                        updatedValues: true,
                                        label: relationObj.displayText.trunc(18),
                                        toolTipLabel: relationObj.displayText,
                                        id: relationObj.guid,
                                        isLineage: true,
                                        isIncomplete: relationObj.isIncomplete,
                                        entityDef: that.getEntityDef(relationObj.typeName)
                                    });
                                    obj["serviceType"] = that.getServiceType({ typeName: relationObj.typeName, entityDef: obj.entityDef });
                                    obj["superTypes"] = that.getNestedSuperTypes({ entityDef: obj.entityDef });
                                    obj['isProcess'] = that.isProcess(obj);
                                    obj['isDeleted'] = that.isDeleted(obj);
                                    return obj;
                                }
                            },
                            crateLineageRelationshipHashMap = function(data) {
                                var that = this,
                                    relations = data && data.relations,
                                    newHashMap = {};
                                _.each(relations, function(obj) {
                                    if (newHashMap[obj.fromEntityId]) {
                                        newHashMap[obj.fromEntityId].push(obj.toEntityId);
                                    } else {
                                        newHashMap[obj.fromEntityId] = [obj.toEntityId];
                                    }
                                });
                                return newHashMap;
                            },
                            getStyleObjStr = function(styleObj) {
                                return 'fill:' + styleObj.fill + ';stroke:' + styleObj.stroke + ';stroke-width:' + styleObj.width;
                            },
                            getNewToNodeRelationship = function(toNodeGuid) {
                                if (toNodeGuid && relationshipMap[toNodeGuid]) {
                                    var newRelationship = [];
                                    _.each(relationshipMap[toNodeGuid], function(guid) {
                                        var nodeToBeUpdated = that.isNodeToBeUpdated(makeNodeData(guidEntityMap[guid]));
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
                                    var toNodeToBeUpdated = that.isNodeToBeUpdated(makeNodeData(guidEntityMap[toNodeGuid]));
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
                            setNode = function(guid) {
                                if (!that.g._nodes[guid]) {
                                    var nodeData = makeNodeData(guidEntityMap[guid]);
                                    that.g.setNode(guid, nodeData);
                                    return nodeData;
                                } else {
                                    return that.g._nodes[guid];
                                }
                            },
                            setEdge = function(fromNodeGuid, toNodeGuid) {
                                that.g.setEdge(fromNodeGuid, toNodeGuid, {
                                    "arrowhead": 'arrowPoint',
                                    "curve": LineageUtils.BezierCurve,
                                    "style": getStyleObjStr(styleObj),
                                    "styleObj": styleObj
                                });
                            },
                            setGraphData = function(fromEntityId, toEntityId) {
                                setNode(fromEntityId);
                                setNode(toEntityId);
                                setEdge(fromEntityId, toEntityId);
                            },
                            pendingFromRelationship = {};
                        if (isHideFilterOn) {
                            var relationshipMap = crateLineageRelationshipHashMap(options)
                            _.each(relationshipMap, function(toNodes, fromNodeGuid) {
                                var fromNodeToBeUpdated = that.isNodeToBeUpdated(makeNodeData(guidEntityMap[fromNodeGuid])),
                                    toNodeList = getToNodeRelation(toNodes, fromNodeToBeUpdated);
                                if (fromNodeToBeUpdated.update) {
                                    if (pendingFromRelationship[fromNodeGuid]) {
                                        pendingFromRelationship[fromNodeGuid] = pendingFromRelationship[fromNodeGuid].concat(toNodeList);
                                    } else {
                                        pendingFromRelationship[fromNodeGuid] = toNodeList;
                                    }
                                } else {
                                    _.each(toNodeList, function(toNodeGuid) {
                                        setGraphData(fromNodeGuid, toNodeGuid);
                                    });
                                }
                            })
                        } else {
                            _.each(relations, function(obj) {
                                setGraphData(obj.fromEntityId, obj.toEntityId);
                            });
                        }
                        if (this.g._nodes[this.guid]) {
                            if (this.g._nodes[this.guid]) {
                                this.g._nodes[this.guid]['isLineage'] = false;
                            }
                            this.findImpactNodeAndUpdateData({ "guid": this.guid, "getStyleObjStr": getStyleObjStr });
                        }
                        resolve(this.g);
                    } catch (e) {
                        reject(e)
                    }
                }.bind(this));
            },
            findImpactNodeAndUpdateData: function(options) {
                var that = this,
                    guid = options.guid,
                    getStyleObjStr = options.getStyleObjStr,
                    traversedMap = {},
                    styleObj = {
                        fill: 'none',
                        stroke: '#fb4200',
                        width: 3
                    },
                    traversed = function(toNodeList, fromNodeGuid) {
                        if (!_.isEmpty(toNodeList)) {
                            if (!traversedMap[fromNodeGuid]) {
                                traversedMap[fromNodeGuid] = true;
                                _.each(toNodeList, function(val, toNodeGuid) {
                                    if (that.g._nodes[toNodeGuid]) {
                                        that.g._nodes[toNodeGuid]['isLineage'] = false;
                                    }
                                    that.g.setEdge(fromNodeGuid, toNodeGuid, {
                                        "arrowhead": 'arrowPoint',
                                        "curve": LineageUtils.BezierCurve,
                                        "style": getStyleObjStr(styleObj),
                                        'styleObj': styleObj
                                    });
                                    traversed(that.g._sucs[toNodeGuid], toNodeGuid);
                                });
                            }
                        }
                    };
                traversed(this.g._sucs[guid], guid)
            },
            zoomed: function(that) {
                this.$('svg').find('>g').attr("transform",
                    "translate(" + this.zoom.translate() + ")" +
                    "scale(" + this.zoom.scale() + ")"
                );
                if (platform.name === "IE") {
                    LineageUtils.refreshGraphForIE({
                        edgeEl: this.$('svg .edgePath')
                    });
                }
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
                if (_.isEmpty(this.g._nodes)) {
                    this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No relations to display</text>');
                    return;
                }
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
                render.arrows().arrowPoint = function(parent, id, edge, type) {
                    return LineageUtils.arrowPointRender(parent, id, edge, type, { guid: that.guid, dagreD3: dagreD3 });
                };
                // Render custom img inside shape
                render.shapes().img = function(parent, bbox, node) {
                    return LineageUtils.imgShapeRender(parent, bbox, node, { guid: that.guid, dagreD3: dagreD3, imageObject: imageObject, $defs: that.svg.select('defs') });
                };
                // Set up an SVG group so that we can translate the final graph.
                if (this.$("svg").find('.output').length) {
                    this.$("svg").find('.output').parent('g').remove();
                }
                var svg = this.svg = d3.select(this.$("svg")[0])
                    .attr("viewBox", "0 0 " + width + " " + height)
                    .attr("enable-background", "new 0 0 " + width + " " + height),
                    svgGroup = svg.append("g");
                // Append defs
                svg.append("defs");
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
                    .attr("transform", "translate(2,-35)")
                    .on('mouseenter', function(d) {
                        d3.select(this).classed("highlight", true);
                    })
                    .on('mouseleave', function(d) {
                        d3.select(this).classed("highlight", false);
                    })
                    .on('click', function(d) {
                        d3.event.preventDefault();
                        tooltip.hide(d);
                        if (that.guid == d) {
                            Utils.notifyInfo({
                                html: true,
                                content: "You are already on " + "<b>" + that.entityName + "</b> detail page."
                            });
                        } else {
                            Utils.setUrl({
                                url: '#!/detailPage/' + d + '?tabActive=lineage',
                                mergeBrowserUrl: false,
                                trigger: true
                            });
                        }
                    });
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
                        tooltip.hide(d);
                        that.onClickNodeToggler({ obj: d });
                        $(el).find('circle').addClass('node-detail-highlight');
                        that.updateRelationshipDetails({ guid: d });
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
                var that = this;
                return new Promise(function(resolve, reject) {
                    try {
                        var data = [],
                            typeStr = '<option></option>';
                        if (!_.isEmpty(that.lineageData)) {
                            _.each(that.lineageData.guidEntityMap, function(obj, index) {
                                var nodeData = that.g._nodes[obj.guid];
                                if ((that.filterObj.isProcessHideCheck || that.filterObj.isDeletedEntityHideCheck) && nodeData && (nodeData.isProcess || nodeData.isDeleted)) {
                                    return;
                                }
                                typeStr += '<option value="' + obj.guid + '">' + obj.displayText + '</option>';
                            });
                        }
                        that.ui.lineageTypeSearch.html(typeStr);
                        that.initilizelineageTypeSearch();
                        resolve();
                    } catch (e) {
                        console.log(e);
                        reject(e);
                    }
                })
            },
            initilizelineageTypeSearch: function() {
                var that = this;
                this.ui.lineageTypeSearch.select2({
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
                if (this.searchNodeObj.selectedNode) {
                    this.ui.lineageTypeSearch.val(this.searchNodeObj.selectedNode);
                    this.ui.lineageTypeSearch.trigger("change.select2");
                }
            },
            updateRelationshipDetails: function(options) {
                var that = this,
                    guid = options.guid,
                    initialData = that.g._nodes[guid],
                    typeName = initialData.typeName || guid,
                    attributeDefs = initialData && initialData.entityDef ? initialData.entityDef.attributeDefs : null;
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
                var that = this,
                    loaderTargetDiv = $(e.currentTarget).find('>i');
                if (loaderTargetDiv.hasClass('fa-refresh')) {
                    Utils.notifyWarn({
                        content: "Please wait while the lineage gets downloaded"
                    });
                    return false; // return if the lineage is not loaded.
                }
                this.toggleLoader(loaderTargetDiv);
                Utils.notifyInfo({
                    content: "Lineage will be downloaded in a moment."
                });
                var entityAttributes = that.entity && that.entity.attributes;
                LineageUtils.SaveSvg(e, {
                    svg: that.$('svg')[0],
                    svgWidth: that.$('svg').width(),
                    svgHeight: that.$('svg').height(),
                    toggleLoader: function() {
                        that.toggleLoader(loaderTargetDiv);
                    },
                    downloadFileName: ((entityAttributes && (entityAttributes.qualifiedName || entityAttributes.name) || "lineage_export") + ".png")
                })
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