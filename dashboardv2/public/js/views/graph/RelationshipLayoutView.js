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

define([
    "require",
    "backbone",
    "hbs!tmpl/graph/RelationshipLayoutView_tmpl",
    "collection/VLineageList",
    "models/VEntity",
    "utils/Utils",
    "utils/CommonViewFunction",
    "d3",
    "d3-tip",
    "utils/Enums",
    "utils/UrlLinks",
    "platform"
], function(require, Backbone, RelationshipLayoutViewtmpl, VLineageList, VEntity, Utils, CommonViewFunction, d3, d3Tip, Enums, UrlLinks, platform) {
    "use strict";

    var RelationshipLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends RelationshipLayoutView */
        {
            _viewName: "RelationshipLayoutView",

            template: RelationshipLayoutViewtmpl,
            className: "resizeGraph",
            /** Layout sub regions */
            regions: {
                relationshipCardsView: '[data-id="relationshipCardsView"]'
            },

            /** ui selector cache */
            ui: {
                relationshipDetailClose: '[data-id="close"]',
                searchNode: '[data-id="searchNode"]',
                relationshipViewToggle: 'input[name="relationshipViewToggle"]',
                relationshipDetailTable: "[data-id='relationshipDetailTable']",
                relationshipDetailTableContainer: ".relationship-detail-table",
                relationshipSVG: "[data-id='relationshipSVG']",
                relationshipDetailValue: "[data-id='relationshipDetailValue']",
                zoomControl: "[data-id='zoomControl']",
                boxClose: '[data-id="box-close"]',
                noValueToggle: "[data-id='noValueToggle']",
                relationshipCardsView: "[data-id='relationshipCardsView']"
            },

            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.relationshipDetailClose] = function() {
                    this.toggleInformationSlider({ close: true });
                };
                events["keyup " + this.ui.searchNode] = "searchNode";
                events["click " + this.ui.boxClose] = "toggleBoxPanel";
                events["change " + this.ui.relationshipViewToggle] = function(e) {
                    this.relationshipViewToggle(e.currentTarget.checked);
                };
                events["click " + this.ui.noValueToggle] = function(e) {
                    Utils.togglePropertyRelationshipTableEmptyValues({
                        inputType: this.ui.noValueToggle,
                        tableEl: this.ui.relationshipDetailValue
                    });
                    this.updateCardsShowEmptyValues(e.currentTarget.checked);
                };
                return events;
            },

            /**
             * intialize a new RelationshipLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, "entity", "entityName", "guid", "actionCallBack", "attributeDefs", "referredEntities", "entityDefCollection"));
                this.graphData = this.createData(this.entity);
                this.relationshipCardCounts = {};
                this.relationshipLoadedCounts = {};
                this.cardsViewLoadInProgress = false;
                
                this.handleRelationshipDataUpdate = _.bind(function(payload) {
                    var relationshipAttributes = payload && payload.data ? payload.data : payload;
                    var relationshipCounts = payload && payload.counts ? payload.counts : this.relationshipCardCounts;
                    var loadedCounts = payload && payload.loadedCounts ? payload.loadedCounts : this.relationshipLoadedCounts;
                    var referredEntities = payload && payload.referredEntities ? payload.referredEntities : null;
                    
                    if (!relationshipAttributes) {
                        return;
                    }
                    this.entity.relationshipAttributes = relationshipAttributes;
                    this.relationshipCardCounts = relationshipCounts || {};
                    this.relationshipLoadedCounts = loadedCounts || {};
                    if (referredEntities) {
                        this.referredEntities = _.extend({}, this.referredEntities, referredEntities);
                    }
                    this.graphData = this.createData(this.entity);
                    if (!this.ui.relationshipViewToggle.is(':checked')) {
                        this.$("svg").empty();
                        if (this.graphData && _.isEmpty(this.graphData.links)) {
                            this.noRelationship();
                        } else if (this.graphData) {
                            this.createGraph(this.graphData);
                        }
                    }
                }, this);
                this.handleRelationshipLoading = _.bind(function(isLoading) {
                    if (isLoading) {
                        this.$(".fontLoader").show();
                    } else {
                        this.$(".fontLoader").hide();
                    }
                }, this);
            },
            createData: function(entity) {
                var that = this,
                    links = [],
                    nodes = {};
                if (entity && entity.relationshipAttributes) {
                    _.each(entity.relationshipAttributes, function(obj, key) {
                        var relationValue = obj;
                        if (relationValue && relationValue.entities) {
                            relationValue = relationValue.entities;
                        }
                        if (relationValue && !_.isArray(relationValue)) {
                            relationValue = [relationValue];
                        }
                        if (!_.isEmpty(relationValue)) {
                            links.push({
                                source: nodes[that.entity.typeName] ||
                                    (nodes[that.entity.typeName] = _.extend({ name: that.entity.typeName }, { value: entity })),
                                target: nodes[key] ||
                                    (nodes[key] = _.extend({
                                        name: key
                                    }, { value: relationValue })),
                                value: relationValue
                            });
                        }
                    });
                }
                return { nodes: nodes, links: links };
            },
            onRender: function() {
                this.isRendered = true;

                // Initialize: show card view by default (checked = Card)
                this.ui.relationshipViewToggle.prop('checked', true);
                this.relationshipViewToggle(true);
            },
            onShow: function(argument) {
                // Always create graph on show if graph view is active
                var isGraphView = !this.ui.relationshipViewToggle.is(':checked');
                if (isGraphView) {
                    if (this.graphData && _.isEmpty(this.graphData.links)) {
                        this.noRelationship();
                    } else if (this.graphData) {
                        this.createGraph(this.graphData);
                    }
                }
                this.createTable();
                
                this.ensureCardsView();
            },
            ensureCardsView: function(forceRefresh) {
                var that = this;
                
                if (this.relationshipCardsViewInstance) {
                    if (forceRefresh) {
                        this.relationshipCardsViewInstance.cardData = {};
                        this.relationshipCardsViewInstance.cardCounts = {};
                        this.relationshipCardsViewInstance.pageLimitByName = {};
                        this.relationshipCardsViewInstance.exhaustedByName = {};
                        this.relationshipCardsViewInstance._lastCardCountSnapshot = {};
                        this.relationshipCardsViewInstance.fetchInitialCards();
                    } else {
                        var hasData = _.keys(this.relationshipCardsViewInstance.cardData || {}).length > 0;
                        if (!hasData) {
                            this.relationshipCardsViewInstance.fetchInitialCards();
                        } else {
                            this.relationshipCardsViewInstance.renderCards();
                        }
                    }
                    return;
                }
                if (this.cardsViewLoadInProgress) {
                    return;
                }
                this.cardsViewLoadInProgress = true;
                require(['views/detail_page/RelationshipCardsLayoutView'], function(RelationshipCardsLayoutView) {
                    try {
                        if (!that.relationshipCardsView) {
                            console.warn("[RelationshipLayoutView] Region not available");
                            that.cardsViewLoadInProgress = false;
                            return;
                        }
                        that.relationshipCardsViewInstance = new RelationshipCardsLayoutView({
                            entity: that.entity,
                            guid: that.guid,
                            attributeDefs: that.attributeDefs,
                            entityDefCollection: that.entityDefCollection,
                            referredEntities: that.referredEntities || {},
                            showEmptyValues: false,
                            onDataLoaded: that.handleRelationshipDataUpdate,
                            onDataLoading: that.handleRelationshipLoading
                        });
                        that.relationshipCardsView.show(that.relationshipCardsViewInstance);
                    } catch (err) {
                        console.error("[RelationshipLayoutView] Error showing cards view:", err);
                    } finally {
                        that.cardsViewLoadInProgress = false;
                    }
                }, function(err) {
                    console.error("[RelationshipLayoutView] Failed to load RelationshipCardsLayoutView:", err);
                    that.cardsViewLoadInProgress = false;
                });
            },
            updateCardsShowEmptyValues: function(showEmptyValues) {
                if (!this.relationshipCardsViewInstance) {
                    return;
                }
                this.relationshipCardsViewInstance.showEmptyValues = !!showEmptyValues;
                this.relationshipCardsViewInstance.renderCards();
            },
            noRelationship: function() {
                this.$("svg").html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No relationship data found</text>');
            },
            toggleInformationSlider: function(options) {
                var panel = this.$(".relationship-node-details");
                if (options && options.close) {
                    panel.removeClass("slide-to-left").addClass("slide-from-left");
                } else {
                    if (panel.hasClass("slide-to-left")) {
                        panel.removeClass("slide-to-left").addClass("slide-from-left");
                    } else {
                        panel.removeClass("slide-from-left").addClass("slide-to-left");
                    }
                }
            },
            toggleBoxPanel: function() {
                this.$(".relationship-node-details").removeClass("slide-to-left").addClass("slide-from-left");
            },
            searchNode: function(e) {
                var searchString = $(e.currentTarget).val(),
                    listString = "",
                    data = this.selectedNodeData,
                    typeName = this.selectedNodeType,
                    activeEntityColor = "#1976d2",
                    deletedEntityColor = "#BB5838",
                    defaultEntityColor = "#e0e0e0",
                    normalizeEntity = function(entity) {
                        if (!entity) {
                            return entity;
                        }
                        if (_.isString(entity)) {
                            entity = { guid: entity };
                        }
                        if (entity.guid && this.referredEntities && this.referredEntities[entity.guid]) {
                            return _.extend({}, entity, this.referredEntities[entity.guid]);
                        }
                        return entity;
                    }.bind(this),
                    getdefault = function(options) {
                        return "<pre class='entity-type-name' style='color:" + options.color + "'>" + options.name + "</pre>";
                    },
                    getWithButton = function(options) {
                        var name = options.name,
                            guid = options.options.guid,
                            entityTypeButton = "";
                        if (guid) {
                            if (options.entity) {
                                entityTypeButton = "<a href='#!/detailPage/" + guid + "' class='entity-type-name' style='color:" + options.color + "'>" + name + "</a>";
                            } else if (options.relationship) {
                                entityTypeButton = "<a href='#/relationshipDetailPage/" + guid + "' class='entity-type-name' style='color:" + options.color + "'>" + name + "</a>";
                            } else {
                                entityTypeButton = "<a href='#!/detailPage/" + guid + "' class='entity-type-name' style='color:" + options.color + "'>" + name + "</a>";
                            }
                        } else {
                            entityTypeButton = "<pre class='entity-type-name' style='color:" + options.color + "'>" + name + "</pre>";
                        }
                        return entityTypeButton;
                    },
                    getEntityTypelist = function(options) {
                        var name = options.entityName ? options.entityName : Utils.getName(options, "displayText"),
                            entityTypeHtml = "";
                        if (options.entityStatus == "ACTIVE" || options.status == "ACTIVE") {
                            if (options.relationshipStatus == "ACTIVE") {
                                entityTypeHtml = getWithButton({
                                    color: activeEntityColor,
                                    options: options,
                                    name: name
                                });
                            } else if (options.relationshipStatus == "DELETED") {
                                entityTypeHtml = getWithButton({
                                    color: activeEntityColor,
                                    options: options,
                                    name: name,
                                    relationship: true
                                });
                            }
                        } else if (options.entityStatus == "DELETED") {
                            entityTypeHtml = getWithButton({
                                color: deletedEntityColor,
                                options: options,
                                name: name,
                                entity: true
                            });
                        } else {
                            entityTypeHtml = getdefault({
                                color: activeEntityColor,
                                options: options,
                                name: name
                            });
                        }
                        return entityTypeHtml + "</pre>";
                    };
                this.ui.searchNode.hide();
                this.$("[data-id='typeName']").text(typeName);
                var getElement = function(options) {
                    var name = options.entityName ? options.entityName : Utils.getName(options, "displayText");
                    var entityTypeButton = getEntityTypelist(options);
                    return entityTypeButton;
                };
                var buildEntityObj = function(item) {
                    var normalized = normalizeEntity(item);
                    var ref = normalized && normalized.guid ? this.referredEntities && this.referredEntities[normalized.guid] : null;
                    var displayText = (ref && (ref.displayText || (ref.attributes && ref.attributes.name))) ||
                        normalized.displayText ||
                        (normalized.attributes && normalized.attributes.name) ||
                        normalized.qualifiedName ||
                        normalized.guid ||
                        "N/A";
                    var typeName = normalized.typeName || (ref && ref.typeName) || this.selectedNodeType;
                    return _.extend({}, ref, normalized, {
                        entityName: displayText,
                        typeName: typeName
                    });
                }.bind(this);
                var buildListItem = function(item) {
                    var name = item.entityName || Utils.getName(item, "displayText");
                    var typeName = item.typeName || "";
                    var displayLabel = typeName ? name + " (" + typeName + ")" : name;
                    var href = item.guid ? "#!/detailPage/" + item.guid + "?tabActive=relationship" : "";
                    var isDeleted = (item.entityStatus || item.status) == "DELETED";
                    var color = isDeleted ? deletedEntityColor : activeEntityColor;
                    var content = href
                        ? "<a href='" + href + "' class='entity-type-name' style='color:" + color + "'>" + _.escape(displayLabel) + "</a>"
                        : "<span class='entity-type-name' style='color:" + color + "'>" + _.escape(displayLabel) + "</span>";
                    return "<li class='entity-list-item'>" + content + "</li>";
                };
                if (_.isArray(data)) {
                    data = _.map(data, function(item) {
                        return buildEntityObj(item);
                    });
                    if (data.length > 1) {
                        this.ui.searchNode.show();
                    }
                    _.each(_.sortBy(data, "entityName"), function(val) {
                        var name = val.entityName || Utils.getName(val, "displayText");
                        var searchTarget = (name + " " + (val.typeName || "")).toLowerCase();
                        if (searchString) {
                            if (searchTarget.includes(searchString.toLowerCase())) {
                                listString += buildListItem(val);
                            } else {
                                return;
                            }
                        } else {
                            listString += buildListItem(val);
                        }
                    });
                } else {
                    data = buildEntityObj(data);
                    listString += buildListItem(data);
                }
                this.$("[data-id='entityList']").html(listString);
            },
            createGraph: function(data) {
                //Ref - http://bl.ocks.org/fancellu/2c782394602a93921faff74e594d1bb1

                var that = this,
                    width = this.$("svg").width() || 1200,
                    height = Math.max(this.$("svg").height() || 600, 600),
                    nodes = d3.values(data.nodes),
                    links = data.links;

                var activeEntityColor = "#00b98b",
                    deletedEntityColor = "#BB5838",
                    defaultEntityColor = "#e0e0e0",
                    selectedNodeColor = "#4a90e2";
                var getNodeCount = function(node) {
                    if (!node) {
                        return 0;
                    }
                    if (node.name === that.entity.typeName) {
                        return 0;
                    }
                    if (that.relationshipLoadedCounts && _.has(that.relationshipLoadedCounts, node.name)) {
                        var count = that.relationshipLoadedCounts[node.name];
                        var parsedCount = _.isNumber(count) ? count : parseInt(count, 10);
                        if (_.isFinite(parsedCount)) {
                            return parsedCount;
                        }
                    }
                    if (_.isArray(node.value)) {
                        return node.value.length;
                    }
                    return node.value ? 1 : 0;
                };

                var svg = d3
                    .select(this.$("svg")[0])
                    .attr("viewBox", "0 0 " + width + " " + height)
                    .attr("enable-background", "new 0 0 " + width + " " + height)
                    .style("min-height", "600px"),
                    node,
                    path;

                var container = svg
                    .append("g")
                    .attr("id", "container")
                    .attr("transform", "translate(0,0)scale(1,1)");

                var zoom = d3
                    .zoom()
                    .scaleExtent([0.1, 4])
                    .on("zoom", function() {
                        container.attr("transform", d3.event.transform);
                    });

                svg.call(zoom).on("dblclick.zoom", null);

                container
                    .append("svg:defs")
                    .selectAll("marker")
                    .data(["deletedLink", "activeLink"]) // Different link/path types can be defined here
                    .enter()
                    .append("svg:marker") // This section adds in the arrows
                    .attr("id", String)
                    .attr("viewBox", "-0 -5 10 10")
                    .attr("refX", 10)
                    .attr("refY", -0.5)
                    .attr("orient", "auto")
                    .attr("markerWidth", 6)
                    .attr("markerHeight", 6)
                    .attr("xoverflow", "visible")
                    .append("svg:path")
                    .attr("d", "M 0,-5 L 10,0 L 0,5")
                    .attr("fill", function(d) {
                        return d === "deletedLink" ? deletedEntityColor : activeEntityColor;
                    })
                    .attr("stroke", "none");

                var simulation = d3
                    .forceSimulation(nodes)
                    .force(
                        "link",
                        d3
                            .forceLink(links)
                            .id(function(d) {
                                return d.name;
                            })
                            .distance(150)
                    )
                    .force("charge", d3.forceManyBody().strength(-300))
                    .force("center", d3.forceCenter(width / 2, height / 2))
                    .force("collision", d3.forceCollide().radius(50));

                    path = container
                    .append("g")
                        .selectAll("path")
                        .data(links)
                        .enter()
                    .append("path")
                        .attr("class", "relatioship-link")
                    .attr("marker-end", function(d) {
                        return isAllEntityRelationDeleted({ data: d, type: "link" }) ? "url(#deletedLink)" : "url(#activeLink)";
                    })
                        .attr("stroke", function(d) {
                        return isAllEntityRelationDeleted({ data: d, type: "link" }) ? deletedEntityColor : activeEntityColor;
                        });

                    node = container
                    .append("g")
                        .selectAll(".node")
                        .data(nodes)
                        .enter()
                        .append("g")
                        .attr("class", "node")
                        .on("mousedown", function() {
                            d3.event.preventDefault();
                        })
                        .on("click", function(d) {
                            if (d3.event.defaultPrevented) return; // ignore drag
                            if (d && d.value && d.value.guid == that.guid) {
                                that.ui.boxClose.trigger("click");
                                return;
                            }
                            that.toggleBoxPanel({ el: that.$(".relationship-node-details") });
                            that.ui.searchNode.data({ obj: d });
                            $(this)
                                .find("circle")
                                .addClass("node-detail-highlight");
                            that.updateRelationshipDetails({ obj: d });
                        })
                        .call(
                            d3
                            .drag()
                            .on("start", dragstarted)
                            .on("drag", dragged)
                            .on("end", dragended)
                    );

                node.append("circle")
                        .attr("r", function() {
                        return 25;
                        })
                        .attr("fill", function(d) {
                        if (d.name === that.entity.typeName) {
                                    return selectedNodeColor;
                            } else {
                            return isAllEntityRelationDeleted({ data: d, type: "node" }) ? deletedEntityColor : activeEntityColor;
                            }
                        })
                    .attr("stroke", "#fff")
                    .attr("stroke-width", "2px")
                    .style("cursor", "pointer")
                    .on("click", function(d) {
                        if (d && d.value && d.value.guid == that.guid) {
                            return;
                        }
                        that.selectedNodeData = d.value;
                        that.selectedNodeType = d.name;
                        
                        // Show the panel
                        var panel = that.$(".relationship-node-details");
                        panel.removeClass("slide-to-left").addClass("slide-from-left");
                        
                        // Trigger after a small delay to ensure DOM is ready
                        setTimeout(function() {
                            panel.removeClass("slide-from-left").addClass("slide-to-left");
                            that.searchNode({ currentTarget: that.ui.searchNode });
                        }, 10);
                        });

                    node.append("text")
                        .attr("x", 0)
                        .attr("y", 0)
                        .attr("dy", function() {
                            return 25 - 17;
                        })
                        .attr("text-anchor", "middle")
                        .style("font-family", "FontAwesome")
                        .style("font-size", "25px")
                        .attr("class", "relationship-node-icon")
                        .text(function(d) {
                            var iconObj = Enums.graphIcon[d.name];
                            if (iconObj && iconObj.textContent) {
                                return iconObj.textContent;
                            }
                            if (d && _.isArray(d.value) && d.value.length > 1) {
                                return "\uf0c5";
                            }
                            return "\uf016";
                        })
                        .attr("fill", "#fff");

                    var countBox = node.append("g");

                    countBox
                        .append("circle")
                        .attr("cx", 18)
                        .attr("cy", -20)
                        .attr("class", "relationship-node-count")
                        .attr("r", function(d) {
                            var count = getNodeCount(d);
                            if (count > 1) {
                                return 9;
                            }
                        });

                    countBox
                        .append("text")
                        .attr("dx", 18)
                        .attr("dy", -16)
                        .attr("text-anchor", "middle")
                        .attr("fill", defaultEntityColor)
                        .attr("class", "relationship-node-count")
                        .text(function(d) {
                            var count = getNodeCount(d);
                            if (count > 1) {
                                return count;
                            }
                        });

                    node
                        .append("text")
                        .attr("x", -15)
                        .attr("y", "35")
                        .attr("class", "relationship-node-label")
                        .text(function(d) {
                            return d.name;
                        });

                simulation.on("tick", function() {
                    path.attr("d", function(d) {
                        var dx = d.target.x - d.source.x,
                            dy = d.target.y - d.source.y,
                            dr = Math.sqrt(dx * dx + dy * dy);
                        return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
                    });

                    node.attr("transform", function(d) {
                        return "translate(" + d.x + "," + d.y + ")";
                    });
                });

                function dragstarted(d) {
                    if (d && d.value && d.value.guid != that.guid) {
                        if (!d3.event.active) simulation.alphaTarget(0.3).restart();
                        d.fx = d.x;
                        d.fy = d.y;
                    }
                }

                function dragged(d) {
                    if (d && d.value && d.value.guid != that.guid) {
                        d.fx = d3.event.x;
                        d.fy = d3.event.y;
                    }
                }

                function dragended(d) {
                    if (d && d.value && d.value.guid != that.guid) {
                        if (!d3.event.active) simulation.alphaTarget(0);
                    }
                }

                function getPathColor(options) {
                    return isAllEntityRelationDeleted(options) ? deletedEntityColor : activeEntityColor;
                }

                function isAllEntityRelationDeleted(options) {
                    var data = options.data,
                        type = options.type;
                    var d = $.extend(true, {}, data);
                    if (d && !_.isArray(d.value)) {
                        d.value = [d.value];
                    }

                    return (
                        _.findIndex(d.value, function(val) {
                            if (type == "node") {
                                return (val.entityStatus || val.status) == "ACTIVE";
                            } else {
                                return val.relationshipStatus == "ACTIVE";
                            }
                        }) == -1
                    );
                }
                var zoomClick = function() {
                    var scaleFactor = 0.8;
                    if (this.id === 'zoom_in') {
                        scaleFactor = 1.3;
                    }
                    zoom.scaleBy(svg.transition().duration(750), scaleFactor);
                }

                d3.selectAll(this.$('.lineageZoomButton')).on('click', zoomClick);
            },
            createTable: function() {
                this.entityModel = new VEntity({});
                var table = CommonViewFunction.propertyTable({
                    scope: this,
                    valueObject: this.entity.relationshipAttributes,
                    attributeDefs: this.attributeDefs
                });
                this.ui.relationshipDetailValue.html(table);
                Utils.togglePropertyRelationshipTableEmptyValues({
                    inputType: this.ui.noValueToggle,
                    tableEl: this.ui.relationshipDetailValue
                });
            },
            relationshipViewToggle: function(checked) {
                var that = this;

                // In the original code: checked = Table, unchecked = Graph
                // So we need to invert the logic
                if (checked) {
                    // Show card view (checked = Table)
                    this.ui.relationshipSVG.addClass("invisible").hide();
                    this.ui.relationshipDetailTableContainer.show();
                    this.ui.zoomControl.hide();
                    this.$el.addClass("auto-height");
                    this.ui.relationshipDetailTable.hide();
                    this.ui.relationshipDetailValue.hide();
                    this.ui.relationshipCardsView.show();
                    
                    // Render card view if not already rendered
                    this.ensureCardsView();
                    
                    // Force a re-render after a short delay to ensure DOM is ready
                    setTimeout(function() {
                        if (that.relationshipCardsViewInstance) {
                            that.relationshipCardsViewInstance.renderCards();
                        }
                    }, 100);
                } else {
                    // Show graph view (unchecked = Graph)
                    this.ui.relationshipSVG.removeClass("invisible").show();
                    this.ui.relationshipDetailTableContainer.hide();
                    this.ui.zoomControl.show();
                    this.$el.removeClass("auto-height");
                    this.ui.relationshipDetailTable.show();
                    this.ui.relationshipDetailValue.show();
                    this.ui.relationshipCardsView.hide();
                    
                    // Ensure graph is created if it hasn't been created yet
                    if (this.graphData && !_.isEmpty(this.graphData.links)) {
                        // Clear existing graph and recreate
                        this.$("svg").empty();
                        this.createGraph(this.graphData);
                    } else if (this.graphData && _.isEmpty(this.graphData.links)) {
                        this.noRelationship();
                    }
                }
            },
            
            onDestroy: function() {
                this.cardsViewLoadInProgress = false;
                if (this.relationshipCardsViewInstance) {
                    this.relationshipCardsViewInstance.destroy();
                    this.relationshipCardsViewInstance = null;
                }
            }
        }
    );
    return RelationshipLayoutView;
});
