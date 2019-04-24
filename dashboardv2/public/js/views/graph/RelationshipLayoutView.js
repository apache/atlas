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
    'hbs!tmpl/graph/RelationshipLayoutView_tmpl',
    'collection/VLineageList',
    'models/VEntity',
    'utils/Utils',
    'utils/CommonViewFunction',
    'd3-tip',
    'utils/Enums',
    'utils/UrlLinks',
    'platform'
], function(require, Backbone, RelationshipLayoutViewtmpl, VLineageList, VEntity, Utils, CommonViewFunction, d3Tip, Enums, UrlLinks, platform) {
    'use strict';

    var RelationshipLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends RelationshipLayoutView */
        {
            _viewName: 'RelationshipLayoutView',

            template: RelationshipLayoutViewtmpl,
            className: "resizeGraph",
            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                relationshipDetailClose: '[data-id="close"]',
                searchNode: '[data-id="searchNode"]',
                relationshipViewToggle: 'input[name="relationshipViewToggle"]',
                relationshipDetailTable: "[data-id='relationshipDetailTable']",
                relationshipSVG: "[data-id='relationshipSVG']",
                relationshipDetailValue: "[data-id='relationshipDetailValue']",
                zoomControl: "[data-id='zoomControl']",
                boxClose: '[data-id="box-close"]'
            },

            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.relationshipDetailClose] = function() {
                    this.toggleInformationSlider({ close: true });
                };
                events["keyup " + this.ui.searchNode] = 'searchNode';
                events["click " + this.ui.boxClose] = 'toggleBoxPanel';
                events["change " + this.ui.relationshipViewToggle] = function(e) {
                    this.relationshipViewToggle(e.currentTarget.checked)
                };
                return events;
            },

            /**
             * intialize a new RelationshipLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'entity', 'entityName', 'guid', 'actionCallBack', 'attributeDefs'));
                this.graphData = this.createData(this.entity);
            },
            createData: function(entity) {
                var that = this,
                    links = [],
                    nodes = {};
                if (entity && entity.relationshipAttributes) {
                    _.each(entity.relationshipAttributes, function(obj, key) {
                        if (!_.isEmpty(obj)) {
                            links.push({
                                "source": nodes[that.entity.typeName] ||
                                    (nodes[that.entity.typeName] = _.extend({ "name": that.entity.typeName }, { value: entity })),
                                "target": nodes[key] ||
                                    (nodes[key] = _.extend({
                                        "name": key
                                    }, { value: obj })),
                                "value": obj
                            })
                        }
                    });
                }
                return { nodes: nodes, links: links };
            },
            onRender: function() {
                this.ui.zoomControl.hide();
                this.$el.addClass('auto-height');
            },
            onShow: function(argument) {
                if (this.graphData && _.isEmpty(this.graphData.links)) {
                    this.noRelationship();
                } else {
                    this.createGraph(this.graphData);
                }
                this.createTable();
            },
            noRelationship: function() {
                this.$('svg').html('<text x="50%" y="50%" alignment-baseline="middle" text-anchor="middle">No relationship data found</text>');
            },
            toggleInformationSlider: function(options) {
                if (options.open && !this.$('.relationship-details').hasClass("open")) {
                    this.$('.relationship-details').addClass('open');
                } else if (options.close && this.$('.relationship-details').hasClass("open")) {
                    d3.selectAll('circle').attr("stroke", "none");
                    this.$('.relationship-details').removeClass('open');
                }
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
            searchNode: function(e) {
                var $el = $(e.currentTarget);
                this.updateRelationshipDetails(_.extend({}, $el.data(), { searchString: $el.val() }))
            },
            updateRelationshipDetails: function(options) {
                var data = options.obj.value,
                    typeName = data.typeName || options.obj.name,
                    searchString = options.searchString,
                    listString = "",
                    getEntityTypelist = function(options) {
                        var activeEntityColor = "#4a90e2",
                            deletedEntityColor = "#BB5838",
                            entityTypeHtml = '',
                            getdefault = function(obj) {
                                var options = obj.options,
                                    status = (Enums.entityStateReadOnly[options.entityStatus || options.status] ? " deleted-relation" : ''),
                                    guid = options.guid,
                                    entityColor = obj.color,
                                    name = obj.name,
                                    typeName = options.typeName;

                                return "<li class=" + status + ">" +
                                    "<a style='color:" + entityColor + "' href=#!/detailPage/" + guid + "?tabActive=relationship>" + name + " (" + typeName + ")</a>" +
                                    "</li>";
                            },
                            getWithButton = function(obj) {
                                var options = obj.options,
                                    status = (Enums.entityStateReadOnly[options.entityStatus || options.status] ? " deleted-relation" : ''),
                                    guid = options.guid,
                                    entityColor = obj.color,
                                    name = obj.name,
                                    typeName = options.typeName,
                                    relationship = obj.relationship || false,
                                    entity = obj.entity || false,
                                    icon = '<i class="fa fa-trash"></i>',
                                    title = "Deleted";
                                if (relationship) {
                                    icon = '<i class="fa fa-long-arrow-right"></i>';
                                    status = (Enums.entityStateReadOnly[options.relationshipStatus || options.status] ? "deleted-relation" : '');
                                    title = "Relationship Deleted";
                                }
                                return "<li class=" + status + ">" +
                                    "<a style='color:" + entityColor + "' href=#!/detailPage/" + options.guid + "?tabActive=relationship>" + _.escape(name) + " (" + options.typeName + ")</a>" +
                                    '<button type="button" title="' + title + '" class="btn btn-sm deleteBtn deletedTableBtn btn-action ">' + icon + '</button>' +
                                    "</li>";
                            };

                        var name = options.entityName ? options.entityName : Utils.getName(options, "displayText");
                        if (options.entityStatus == "ACTIVE") {
                            if (options.relationshipStatus == "ACTIVE") {
                                entityTypeHtml = getdefault({
                                    "color": activeEntityColor,
                                    "options": options,
                                    "name": _.escape(name)
                                });
                            } else if (options.relationshipStatus == "DELETED") {
                                entityTypeHtml = getWithButton({
                                    "color": activeEntityColor,
                                    "options": options,
                                    "name": _.escape(name),
                                    "relationship": true
                                })
                            }
                        } else if (options.entityStatus == "DELETED") {
                            entityTypeHtml = getWithButton({
                                "color": deletedEntityColor,
                                "options": options,
                                "name": _.escape(name),
                                "entity": true
                            })
                        } else {

                            entityTypeHtml = getdefault({
                                "color": activeEntityColor,
                                "options": options,
                                "name": _.escape(name)
                            });
                        }
                        return entityTypeHtml;
                    };
                this.ui.searchNode.hide();
                this.$("[data-id='typeName']").text(typeName);
                var getElement = function(options) {
                    var name = options.entityName ? options.entityName : Utils.getName(options, "displayText");
                    var entityTypeButton = getEntityTypelist(options);
                    return entityTypeButton;
                }
                if (_.isArray(data)) {
                    if (data.length > 1) {
                        this.ui.searchNode.show();
                    }
                    _.each(_.sortBy(data, "displayText"), function(val) {
                        var name = Utils.getName(val, "displayText"),
                            valObj = _.extend({}, val, { entityName: name });
                        if (searchString) {
                            if (name.search(new RegExp(searchString, "i")) != -1) {
                                listString += getElement(valObj);
                            } else {
                                return;
                            }
                        } else {
                            listString += getElement(valObj);
                        }
                    });
                } else {
                    listString += getElement(data);
                }
                this.$("[data-id='entityList']").html(listString);
            },
            createGraph: function(data) {
                var that = this,
                    width = this.$('svg').width(),
                    height = this.$('svg').height();

                var scale = 1.0,
                    activeEntityColor = "#00b98b",
                    deletedEntityColor = "#BB5838",
                    defaultEntityColor = "#e0e0e0",
                    selectedNodeColor = "#4a90e2";

                var force = d3.layout.force()
                    .nodes(d3.values(data.nodes))
                    .links(data.links)
                    .size([width, height])
                    .linkDistance(200)
                    .gravity(0.0)
                    .friction(0.1)
                    .charge(function(d) {
                        var charge = -500;
                        if (d.index === 0) charge = 100
                        return charge;
                    })
                    .on("tick", tick)
                    .start();

                var zoom = d3.behavior.zoom()
                    .scale(scale)
                    .scaleExtent([1, 5])
                    .on("zoom", zoomed);

                function zoomed() {
                    container.attr("transform",
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
                        factor = 0.5,
                        target_zoom = 1,
                        center = [width / 2, height / 2],
                        extent = zoom.scaleExtent(),
                        translate = zoom.translate(),
                        translate0 = [],
                        l = [],
                        view = { x: translate[0], y: translate[1], k: zoom.scale() };

                    d3.event.preventDefault();
                    direction = (this.id === 'zoom_in') ? 1 : -1;
                    target_zoom = zoom.scale() * (1 + factor * direction);

                    if (target_zoom < extent[0] || target_zoom > extent[1]) { return false; }

                    translate0 = [(center[0] - view.x) / view.k, (center[1] - view.y) / view.k];
                    view.k = target_zoom;
                    l = [translate0[0] * view.k + view.x, translate0[1] * view.k + view.y];

                    view.x += center[0] - l[0];
                    view.y += center[1] - l[1];

                    interpolateZoom([view.x, view.y], view.k);
                }



                d3.selectAll(this.$('.lineageZoomButton')).on('click', zoomClick);

                var svg = d3.select(this.$("svg")[0])
                    .attr("viewBox", "0 0 " + width + " " + height)
                    .attr("enable-background", "new 0 0 " + width + " " + height)
                    .call(zoom)
                    .on("dblclick.zoom", null),
                    drag = force.drag()
                    .on("dragstart", dragstart);

                var container = svg.append("g")
                    .attr("id", "container")
                    .attr("transform", "translate(0,0)scale(1,1)");


                // build the arrow.
                container.append("svg:defs").selectAll("marker")
                    .data(["deletedLink", "activeLink"]) // Different link/path types can be defined here
                    .enter().append("svg:marker") // This section adds in the arrows
                    .attr("id", String)
                    .attr("viewBox", "0 -5 10 10")
                    .attr("refX", 10)
                    .attr("refY", -0.5)
                    .attr("markerWidth", 6)
                    .attr("markerHeight", 6)
                    .attr("orient", "auto")
                    .append("svg:path")
                    .attr("d", "M0,-5L10,0L0,5")
                    .attr("fill", function(d) {
                        return d == "deletedLink" ? deletedEntityColor : activeEntityColor;
                    });

                function getPathColor(options) {
                    return isAllEntityRelationDeleted(options) ? deletedEntityColor : activeEntityColor
                }

                function isAllEntityRelationDeleted(options) {
                    var data = options.data,
                        type = options.type;
                    var d = $.extend(true, {}, data);
                    if (d && !_.isArray(d.value)) {
                        d.value = [d.value];
                    }

                    return (_.findIndex(d.value, function(val) {
                        if (type == "node") {
                            return (val.entityStatus || val.status) == "ACTIVE"
                        } else {
                            return val.relationshipStatus == "ACTIVE"
                        }
                    }) == -1);
                }

                // add the links and the arrows
                var path = container.append("svg:g").selectAll("path")
                    .data(force.links())
                    .enter().append("svg:path")
                    //    .attr("class", function(d) { return "link " + d.type; })
                    .attr("class", "relatioship-link")
                    .attr("stroke", function(d) { return getPathColor({ data: d, type: 'path' }) })
                    .attr("marker-end", function(d) {
                        return "url(#" + (isAllEntityRelationDeleted({ data: d }) ? "deletedLink" : "activeLink") + ")";
                    });

                // define the nodes
                var node = container.selectAll(".node")
                    .data(force.nodes())
                    .enter().append("g")
                    .attr("class", "node")
                    .on('touchstart', function(d) {
                        if (d && d.value && d.value.guid != that.guid) {
                            d3.event.stopPropagation();
                        }
                    })
                    .on('mousedown', function(d) {
                        if (d && d.value && d.value.guid != that.guid) {
                            d3.event.stopPropagation();
                        }
                    })
                    .on('click', function(d) {
                        if (d3.event.defaultPrevented) return; // ignore drag
                        if (d && d.value && d.value.guid == that.guid) {
                            that.ui.boxClose.trigger('click');
                            return;
                        }
                        that.toggleBoxPanel({ el: that.$('.relationship-node-details') });
                        that.ui.searchNode.data({ obj: d });
                        $(this).find('circle').addClass("node-detail-highlight");
                        that.updateRelationshipDetails({ obj: d });

                    }).call(force.drag);

                // add the nodes
                var circleContainer = node.append("g");
                circleContainer.on("dblclick", function(d) {
                    if ((_.isArray(d.value) && d.value.length == 1) || d.value.guid) {
                        var guid = _.isArray(d.value) ? _.first(d.value).guid : d.value.guid;
                        Utils.setUrl({
                            url: '#!/detailPage/' + guid,
                            mergeBrowserUrl: false,
                            urlParams: { tabActive: 'relationship' },
                            trigger: true
                        });
                    }
                })


                circleContainer.append("circle")
                    .attr("cx", 0)
                    .attr("cy", 0)
                    .attr("r", function(d) {
                        d.radius = 25;
                        return d.radius;
                    })
                    .attr("fill", function(d) {
                        if (d && d.value && d.value.guid == that.guid) {
                            if (isAllEntityRelationDeleted({ data: d, type: 'node' })) {
                                return deletedEntityColor;
                            } else {
                                return selectedNodeColor;
                            }
                        } else if (isAllEntityRelationDeleted({ data: d, type: 'node' })) {
                            return deletedEntityColor;
                        } else {
                            return activeEntityColor;
                        }
                    })
                    .attr("typename", function(d) {
                        return d.name;
                    })
                circleContainer.append("text")
                    .attr('x', 0)
                    .attr('y', 0)
                    .attr('dy', (25 - 17))
                    .attr("text-anchor", "middle")
                    .style("font-family", "FontAwesome")
                    .style('font-size', function(d) { return '25px'; })
                    .text(function(d) {
                        var iconObj = Enums.graphIcon[d.name];
                        if (iconObj && iconObj.textContent) {
                            return iconObj.textContent;
                        } else {
                            if (d && _.isArray(d.value) && d.value.length > 1) {
                                return '\uf0c5';
                            } else {
                                return '\uf016';
                            }
                        }
                    })
                    .attr("fill", function(d) {
                        return "#fff";
                    });
                var countBox = circleContainer.append('g')
                countBox.append("circle")
                    .attr("cx", 18)
                    .attr("cy", -20)
                    .attr("r", function(d) {
                        if (_.isArray(d.value) && d.value.length > 1) {
                            return 10;
                        }
                    });

                countBox.append("text")
                    .attr('dx', 18)
                    .attr('dy', -16)
                    .attr("text-anchor", "middle")
                    .attr("fill", defaultEntityColor)
                    .text(function(d) {
                        if (_.isArray(d.value) && d.value.length > 1) {
                            return d.value.length;
                        }
                    });


                // add the text 
                node.append("text")
                    .attr("x", -15)
                    .attr("y", "35")
                    .text(function(d) { return d.name; });

                // add the curvy lines
                function tick() {
                    path.attr("d", function(d) {
                        var diffX = d.target.x - d.source.x,
                            diffY = d.target.y - d.source.y,

                            // Length of path from center of source node to center of target node
                            pathLength = Math.sqrt((diffX * diffX) + (diffY * diffY)),

                            // x and y distances from center to outside edge of target node
                            offsetX = (diffX * d.target.radius) / pathLength,
                            offsetY = (diffY * d.target.radius) / pathLength;

                        return "M" + d.source.x + "," + d.source.y + "A" + pathLength + "," + pathLength + " 0 0,1 " + (d.target.x - offsetX) + "," + (d.target.y - offsetY)
                    });

                    node.attr("transform", function(d) {
                        if (d && d.value && d.value.guid == that.guid) {
                            d.x = (width / 2)
                            d.y = (height / 2)
                        }
                        return "translate(" + d.x + "," + d.y + ")";
                    });
                }

                function dragstart(d) {
                    d3.select(this).classed("fixed", d.fixed = true);
                }
            },
            createTable: function() {
                this.entityModel = new VEntity({});
                var table = CommonViewFunction.propertyTable({ scope: this, valueObject: this.entity.relationshipAttributes, attributeDefs: this.attributeDefs });
                this.ui.relationshipDetailValue.html(table);
            },
            relationshipViewToggle: function(checked) {
                this.ui.relationshipDetailTable.toggleClass('visible invisible');
                this.ui.relationshipSVG.toggleClass('visible invisible');

                if (checked) {
                    this.ui.zoomControl.hide();
                    this.$el.addClass('auto-height');
                } else {
                    this.ui.zoomControl.show();
                    this.$el.removeClass('auto-height');
                }

            }
        });
    return RelationshipLayoutView;
});