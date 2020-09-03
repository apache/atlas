/*
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
define([
    "require",
    "backbone",
    "hbs!tmpl/graph/TypeSystemTreeView_tmpl",
    "collection/VLineageList",
    "models/VEntity",
    "LineageHelper",
    "d3",
    "dagreD3",
    "d3-tip",
    "utils/CommonViewFunction",
    "utils/Utils",
    "platform",
    "jquery-ui"
], function(require, Backbone, TypeSystemTreeViewTmpl, VLineageList, VEntity, LineageHelper, d3, dagreD3, d3Tip, CommonViewFunction, Utils, platform) {
    "use strict";

    /** @lends TypeSystemTreeView */
    var TypeSystemTreeView = Backbone.Marionette.LayoutView.extend({
        _viewName: "TypeSystemTreeViewTmpl",

        template: TypeSystemTreeViewTmpl,
        templateHelpers: function() {
            return {
                modalID: this.viewId,
                width: "100%",
                height: "300px"
            };
        },

        /** Layout sub regions */
        regions: {
            RTypeSystemTreeViewPage: "#r_typeSystemTreeViewPage"
        },

        /** ui selector cache */
        ui: {
            typeSystemTreeViewPage: "[data-id='typeSystemTreeViewPage']",
            boxClose: '[data-id="box-close"]',
            nodeDetailTable: '[data-id="nodeDetailTable"]',
            typeSearch: '[data-id="typeSearch"]',
            filterServiceType: '[data-id="filterServiceType"]',
            onZoomIn: '[data-id="zoom-in"]',
            onZoomOut: '[data-id="zoom-out"]',
            filterBox: ".filter-box",
            searchBox: ".search-box",
            filterToggler: '[data-id="filter-toggler"]',
            searchToggler: '[data-id="search-toggler"]',
            reset: '[data-id="reset"]',
            fullscreenToggler: '[data-id="fullScreen-toggler"]'
        },
        /** ui events hash */
        events: function() {
            var events = {};
            events["click " + this.ui.boxClose] = "toggleBoxPanel";
            events["click " + this.ui.onZoomIn] = "onClickZoomIn";
            events["click " + this.ui.onZoomOut] = "onClickZoomOut";
            events["click " + this.ui.filterToggler] = "onClickFilterToggler";
            events["click " + this.ui.searchToggler] = "onClickSearchToggler";
            events["click " + this.ui.fullscreenToggler] = "onClickFullscreenToggler";
            events["click " + this.ui.reset] = "onClickReset";
            return events;
        },

        /**
         * @constructs
         */
        initialize: function(options) {
            _.extend(this, _.pick(options, "entityDefCollection"));
        },
        onShow: function() {
            this.$(".fontLoader").show();
            this.initializeGraph();
            this.fetchGraphData();
        },
        onRender: function() {},
        fetchGraphData: function(options) {
            var that = this;
            var entityTypeDef = that.entityDefCollection.fullCollection.toJSON();
            this.$(".fontLoader").show();
            this.$("svg").empty();
            if (that.isDestroyed) {
                return;
            }
            if (entityTypeDef.length) {
                that.generateData($.extend(true, {}, { data: entityTypeDef }, options)).then(function(graphObj) {
                    that.createGraph();
                });
            }
        },
        generateData: function(options) {
            return new Promise(
                function(resolve, reject) {
                    try {
                        var that = this,
                            newHashMap = {},
                            styleObj = {
                                fill: "none",
                                stroke: "#ffb203",
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
                                        label: relationObj.name.trunc(18),
                                        toolTipLabel: relationObj.name,
                                        id: relationObj.guid,
                                        isLineage: true,
                                        isIncomplete: false
                                    });
                                    return obj;
                                }
                            },
                            getStyleObjStr = function(styleObj) {
                                return "fill:" + styleObj.fill + ";stroke:" + styleObj.stroke + ";stroke-width:" + styleObj.width;
                            },
                            setNode = function(guid, obj) {
                                var node = that.LineageHelperRef.getNode(guid);
                                if (!node) {
                                    var nodeData = makeNodeData(obj);
                                    that.LineageHelperRef.setNode(guid, nodeData);
                                    return nodeData;
                                } else {
                                    return node;
                                }
                            },
                            setEdge = function(fromNodeGuid, toNodeGuid) {
                                that.LineageHelperRef.setEdge(fromNodeGuid, toNodeGuid, {
                                    arrowhead: "arrowPoint",
                                    style: getStyleObjStr(styleObj),
                                    styleObj: styleObj
                                });
                            },
                            setGraphData = function(fromEntityId, toEntityId) {
                                setNode(fromEntityId);
                                setNode(toEntityId);
                                setEdge(fromEntityId, toEntityId);
                            };

                        if (options.data) {
                            if (options.filter) {
                                var pendingSubList = {},
                                    pendingSuperList = {},
                                    temp = {},
                                    doneList = {},
                                    traveseSubSuper = function(obj, ignoreSubTypes) {
                                        var fromEntityId = obj.guid;
                                        if (!ignoreSubTypes && obj.subTypes.length) {
                                            _.each(obj.subTypes, function(subType) {
                                                var tempObj = doneList[subType] || temp[subType];
                                                if (tempObj) {
                                                    setNode(tempObj.guid, tempObj);
                                                    setEdge(fromEntityId, tempObj.guid);
                                                } else {
                                                    if (pendingSubList[subType]) {
                                                        pendingSubList[subType].push(fromEntityId);
                                                    } else {
                                                        pendingSubList[subType] = [fromEntityId];
                                                    }
                                                }
                                            });
                                        }
                                        if (obj.superTypes.length) {
                                            _.each(obj.superTypes, function(superType) {
                                                var tempObj = doneList[superType] || temp[superType];
                                                if (tempObj) {
                                                    setNode(tempObj.guid, tempObj);
                                                    setEdge(tempObj.guid, fromEntityId);
                                                    if (tempObj.superTypes.length) {
                                                        traveseSubSuper(tempObj, true);
                                                    }
                                                } else {
                                                    if (pendingSuperList[superType]) {
                                                        pendingSuperList[superType].push(fromEntityId);
                                                    } else {
                                                        pendingSuperList[superType] = [fromEntityId];
                                                    }
                                                }
                                            });
                                        }
                                    };
                                _.each(options.data, function(obj) {
                                    var fromEntityId = obj.guid;
                                    if (obj.serviceType === options.filter) {
                                        doneList[obj.name] = obj;
                                        setNode(fromEntityId, obj);
                                        if (pendingSubList[obj.name]) {
                                            _.map(pendingSubList[obj.name], function(guid) {
                                                setEdge(guid, fromEntityId);
                                            });
                                            delete pendingSubList[obj.name];
                                        }
                                        if (pendingSuperList[obj.name]) {
                                            _.map(pendingSuperList[obj.name], function(guid) {
                                                setEdge(fromEntityId, guid);
                                            });
                                            delete pendingSuperList[obj.name];
                                        }
                                        traveseSubSuper(obj);
                                    } else {
                                        if (pendingSubList[obj.name]) {
                                            setNode(fromEntityId, obj);
                                            doneList[obj.name] = obj;
                                            _.map(pendingSubList[obj.name], function(guid) {
                                                setEdge(guid, fromEntityId);
                                            });
                                            delete pendingSubList[obj.name];
                                        }
                                        if (pendingSuperList[obj.name]) {
                                            var fromEntityId = obj.guid;
                                            setNode(fromEntityId, obj);
                                            doneList[obj.name] = obj;
                                            _.map(pendingSuperList[obj.name], function(guid) {
                                                setEdge(fromEntityId, guid);
                                            });
                                            delete pendingSuperList[obj.name];
                                        }
                                        if (!doneList[obj.name]) {
                                            temp[obj.name] = obj;
                                        }
                                    }
                                });
                                pendingSubList = null;
                                pendingSuperList = null;
                                doneList = null;
                            } else {
                                var pendingList = {},
                                    doneList = {};

                                _.each(options.data, function(obj) {
                                    var fromEntityId = obj.guid;
                                    doneList[obj.name] = obj;
                                    setNode(fromEntityId, obj);
                                    if (pendingList[obj.name]) {
                                        _.map(pendingList[obj.name], function(guid) {
                                            setEdge(guid, fromEntityId);
                                        });
                                        delete pendingList[obj.name];
                                    }
                                    if (obj.subTypes.length) {
                                        _.each(obj.subTypes, function(subTypes) {
                                            //var subTypesObj = _.find(options.data({ name: superTypes });
                                            //setNode(superTypeObj.attributes.guid, superTypeObj.attributes);
                                            if (doneList[subTypes]) {
                                                setEdge(fromEntityId, doneList[subTypes].guid);
                                            } else {
                                                if (pendingList[subTypes]) {
                                                    pendingList[subTypes].push(fromEntityId);
                                                } else {
                                                    pendingList[subTypes] = [fromEntityId];
                                                }
                                            }
                                        });
                                    }
                                });
                                pendingList = null;
                                doneList = null;
                            }
                        }
                        resolve(this.g);
                    } catch (e) {
                        reject(e);
                    }
                }.bind(this)
            );
        },
        toggleBoxPanel: function(options) {
            var el = options && options.el,
                nodeDetailToggler = options && options.nodeDetailToggler,
                currentTarget = options.currentTarget;
            this.$el.find(".show-box-panel").removeClass("show-box-panel");
            if (el && el.addClass) {
                el.addClass("show-box-panel");
            }
            this.$("circle.node-detail-highlight").removeClass("node-detail-highlight");
        },
        onClickNodeToggler: function(options) {
            this.toggleBoxPanel({ el: this.$(".lineage-node-detail"), nodeDetailToggler: true });
        },
        onClickZoomIn: function() {
            this.LineageHelperRef.zoomIn();
        },
        onClickZoomOut: function() {
            this.LineageHelperRef.zoomOut();
        },
        onClickFilterToggler: function() {
            this.toggleBoxPanel({ el: this.ui.filterBox });
        },
        onClickSearchToggler: function() {
            this.toggleBoxPanel({ el: this.ui.searchBox });
        },
        onClickReset: function() {
            this.fetchGraphData({ refresh: true });
        },
        onClickFullscreenToggler: function(e) {
            var icon = $(e.currentTarget).find("i"),
                panel = $(e.target).parents(".tab-pane").first();
            icon.toggleClass("fa-expand fa-compress");
            if (icon.hasClass("fa-expand")) {
                icon.parent("button").attr("data-original-title", "Full Screen");
            } else {
                icon.parent("button").attr("data-original-title", "Default View");
            }
            panel.toggleClass("fullscreen-mode");
        },
        updateDetails: function(data) {
            this.$("[data-id='typeName']").text(Utils.getName(data));
            delete data.id;
            //atttributes
            data["atttributes"] = (data.attributeDefs || []).map(function(obj) {
                return obj.name;
            });
            delete data.attributeDefs;
            //businessAttributes
            data["businessAttributes"] = _.keys(data.businessAttributeDefs);
            delete data.businessAttributeDefs;
            //relationshipAttributes
            data["relationshipAttributes"] = (data.relationshipAttributeDefs || []).map(function(obj) {
                return obj.name;
            });
            delete data.relationshipAttributeDefs;

            console.log(data);

            this.ui.nodeDetailTable.html(
                CommonViewFunction.propertyTable({
                    scope: this,
                    guidHyperLink: false,
                    getEmptyString: function(key) {
                        if (key === "subTypes" || key === "superTypes" || key === "atttributes" || key === "relationshipAttributes") {
                            return "[]";
                        }
                        return "N/A";
                    },
                    valueObject: _.omit(data, ["isLineage", "isIncomplete", "label", "shape", "toolTipLabel", "updatedValues"]),
                    sortBy: true
                })
            );
        },
        createGraph: function(refresh) {
            this.LineageHelperRef.createGraph();
        },
        filterData: function(value) {
            this.LineageHelperRef.refresh();
            this.fetchGraphData({ filter: value });
        },
        initializeGraph: function() {
            //ref - https://bl.ocks.org/seemantk/80613e25e9804934608ac42440562168
            var that = this,
                node = this.$("svg.main").parent()[0].getBoundingClientRect();
            this.$("svg").attr("viewBox", "0 0 " + node.width + " " + node.height);
            this.LineageHelperRef = new LineageHelper.default({
                el: this.$("svg.main")[0],
                legends: false,
                setDataManually: true,
                width: node.width,
                height: node.height,
                isShowHoverPath: true,
                zoom: true,
                fitToScreen: true,
                dagreOptions: {
                    rankdir: "tb"
                },
                onNodeClick: function(d) {
                    that.onClickNodeToggler();
                    that.updateDetails(that.LineageHelperRef.getNode(d.clickedData, true));
                },
                beforeRender: function() {
                    that.$(".fontLoader").show();
                },
                afterRender: function() {
                    that.graphOptions = that.LineageHelperRef.getGraphOptions();
                    that.renderTypeFilterSearch();
                    that.$(".fontLoader").hide();
                    return;
                }
            });
        },
        renderTypeFilterSearch: function(data) {
            var that = this;
            var searchStr = "<option></option>",
                filterStr = "<option></option>",
                tempFilteMap = {};
            var nodes = that.LineageHelperRef.getNodes();
            if (!_.isEmpty(nodes)) {
                _.each(nodes, function(obj) {
                    searchStr += '<option value="' + obj.guid + '">' + obj.name + "</option>";
                    if (obj.serviceType && !tempFilteMap[obj.serviceType]) {
                        tempFilteMap[obj.serviceType] = obj.serviceType;
                        filterStr += '<option value="' + obj.serviceType + '">' + obj.serviceType + "</option>";
                    }
                });
            }
            this.ui.typeSearch.html(searchStr);
            if (!this.ui.filterServiceType.data("select2")) {
                this.ui.filterServiceType.html(filterStr);
            }

            this.initilizeTypeFilterSearch();
        },
        initilizeTypeFilterSearch: function() {
            var that = this;
            this.ui.typeSearch
                .select2({
                    closeOnSelect: true,
                    placeholder: "Select Node"
                })
                .on("change.select2", function(e) {
                    e.stopPropagation();
                    e.stopImmediatePropagation();
                    var selectedNode = $('[data-id="typeSearch"]').val();
                    //that.searchNodeObj.selectedNode = selectedNode;
                    that.LineageHelperRef.searchNode({ guid: selectedNode });
                });
            if (!this.ui.filterServiceType.data("select2")) {
                this.ui.filterServiceType
                    .select2({
                        closeOnSelect: true,
                        placeholder: "Select ServiceType"
                    })
                    .on("change.select2", function(e) {
                        e.stopPropagation();
                        e.stopImmediatePropagation();
                        var selectedNode = $('[data-id="filterServiceType"]').val();
                        that.filterData(selectedNode);
                        //that.searchNodeObj.selectedNode = selectedNode;
                        //that.LineageHelperRef.searchNode({ guid: selectedNode });
                    });
                // if (this.searchNodeObj.selectedNode) {
                //     this.ui.typeSearch.val(this.searchNodeObj.selectedNode);
                //     this.ui.typeSearch.trigger("change.select2");
                // }
            }
        }
    });
    return TypeSystemTreeView;
});