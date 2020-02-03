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
    "hbs!tmpl/search/tree/NameSpaceTreeLayoutView_tmpl",
    "utils/Utils",
    "utils/Messages",
    "utils/Globals",
    "utils/UrlLinks",
    "utils/CommonViewFunction",
    "collection/VSearchList",
    "collection/VGlossaryList",
    "utils/Enums",
    "jstree"
], function(require, NameSpaceTreeLayoutViewTmpl, Utils, Messages, Globals, UrlLinks, CommonViewFunction, VSearchList, VGlossaryList, Enums) {
    "use strict";

    var NameSpaceTreeLayoutView = Marionette.LayoutView.extend({
        template: NameSpaceTreeLayoutViewTmpl,

        regions: {},
        ui: {
            //refresh
            refreshTree: '[data-id="refreshTree"]',

            // tree el
            nameSpaceSearchTree: '[data-id="nameSpaceSearchTree"]',

            // Create
            createNameSpace: '[data-id="createNameSpace"]'
        },
        templateHelpers: function() {
            return {
                apiBaseUrl: UrlLinks.apiBaseUrl
            };
        },
        events: function() {
            var events = {},
                that = this;
            // refresh individual tree
            events["click " + this.ui.refreshTree] = function(e) {
                var type = $(e.currentTarget).data("type");
                e.stopPropagation();
                that.refresh({ type: type });
            };

            events["click " + this.ui.createNameSpace] = function(e) {
                e.stopPropagation();
                that.triggerUrl("#!/administrator?tabActive=namespace");
            };

            return events;
        },
        initialize: function(options) {
            this.options = options;
            _.extend(
                this,
                _.pick(
                    options,
                    "typeHeaders",
                    "namespaceID",
                    "searchVent",
                    "entityDefCollection",
                    "enumDefCollection",
                    "nameSpaceCollection",
                    "searchTableColumns",
                    "searchTableFilters",
                    "metricCollection",
                    "nameSpaceCollection"
                )
            );
            this.bindEvents();
        },
        onRender: function() {
            this.renderNameSpaceTree();
            //this.createNameSpaceAction();
        },
        bindEvents: function() {
            var that = this;
            this.listenTo(
                this.nameSpaceCollection.fullCollection,
                "reset add remove",
                function() {
                    if (this.ui.nameSpaceSearchTree.jstree(true)) {
                        that.ui.nameSpaceSearchTree.jstree(true).refresh();
                    } else {
                        this.renderNameSpaceTree();
                    }
                },
                this
            );
            // this.options.nameSpaceVent.on("Save:NamespaceAttribute", function(data) {
            //     that.ui.nameSpaceSearchTree.jstree(true).refresh();
            // });
            $("body").on("click", ".namespacePopoverOptions li", function(e) {
                that.$(".nameSpacePopover").popover("hide");
                that[$(this).find("a").data("fn") + "NameSpace"](e);
            });
        },
        createNameSpaceAction: function() {
            var that = this;
            Utils.generatePopover({
                el: this.$el,
                contentClass: "namespacePopoverOptions",
                popoverOptions: {
                    selector: ".nameSpacePopover",
                    content: function() {
                        var type = $(this).data("detail"),
                            liString =
                            "<li><i class='fa fa-list-alt'></i><a href='javascript:void(0)' data-fn='onViewEdit'>View/Edit</a></li><li><i class='fa fa-search'></i><a href='javascript:void(0)' data-fn='onSelectedSearch'>Search</a></li>";
                        return "<ul>" + liString + "</ul>";
                    }
                }
            });
        },
        renderNameSpaceTree: function() {
            this.generateSearchTree({
                $el: this.ui.nameSpaceSearchTree
            });
        },
        manualRender: function(options) {
            var that = this;
            _.extend(this, options);
            if (Utils.getUrlState.isAdministratorTab() && this.namespaceID) {
                this.ui.nameSpaceSearchTree.jstree(true).select_node(this.namespaceID);
            } else {
                this.ui.nameSpaceSearchTree.jstree(true).deselect_all();
                this.namespaceID = null;
            }
            // if (this.options.value === undefined) {
            //     this.options.value = {};
            // }
            // if (!this.options.value.tag) {
            //     this.ui.nameSpaceSearchTree.jstree(true).deselect_all();
            //     this.nameSpaceId = null;
            // } else {
            //     if (that.options.value.tag.indexOf("*") != -1) {
            //         that.ui.nameSpaceSearchTree.jstree(true).deselect_all();
            //     }
            //     var dataFound = this.nameSpaceCollection.fullCollection.find(function(obj) {
            //         return obj.get("name") === that.options.value.tag;
            //     });
            //     if (dataFound) {
            //         if ((this.nameSpaceId && this.nameSpaceId !== dataFound.get("guid")) || this.nameSpaceId === null) {
            //             if (this.nameSpaceId) {
            //                 this.ui.nameSpaceSearchTree.jstree(true).deselect_node(this.nameSpaceId);
            //             }
            //             this.fromManualRender = true;
            //             this.nameSpaceId = dataFound.get("guid");
            //             this.ui.nameSpaceSearchTree.jstree(true).select_node(dataFound.get("guid"));
            //         }
            //     }
            //     if (!dataFound && Globals[that.options.value.tag]) {
            //         this.fromManualRender = true;
            //         this.typeId = Globals[that.options.value.tag].guid;
            //         this.ui.nameSpaceSearchTree.jstree(true).select_node(this.typeId);
            //     }
            // }
        },
        onNodeSelect: function(nodeData) {
            var that = this,
                options = nodeData.node.original,
                url = "#!/administrator/namespace";
            if (options.parent === undefined) {
                url += "/" + options.id;
                this.triggerUrl(url);
            } else {
                //this.triggerSearch();
            }
        },
        onViewEditNameSpace: function() {
            var selectedNode = this.ui.nameSpaceSearchTree.jstree("get_selected", true);
            if (selectedNode && selectedNode[0]) {
                selectedNode = selectedNode[0];
                var url = "#!/administrator?tabActive=namespace";
                if (selectedNode.parent && selectedNode.original && selectedNode.original.name) {
                    url += "&ns=" + selectedNode.parent + "&nsa=" + selectedNode.original.name;
                    this.triggerUrl(url);
                }
            }
        },
        // triggerSearch: function(params, url) {
        //     var serachUrl = url ? url : "#!/search/searchResult";
        //     Utils.setUrl({
        //         url: serachUrl,
        //         urlParams: params,
        //         mergeBrowserUrl: false,
        //         trigger: true,
        //         updateTabState: true
        //     });
        // },
        triggerUrl: function(url) {
            Utils.setUrl({
                url: url,
                mergeBrowserUrl: false,
                trigger: true,
                updateTabState: true
            });
        },
        refresh: function(options) {
            var that = this;
            this.nameSpaceCollection.fetch({
                skipDefaultError: true,
                silent: true,
                complete: function() {
                    that.nameSpaceCollection.fullCollection.comparator = function(model) {
                        return model.get("name").toLowerCase();
                    };
                    that.nameSpaceCollection.fullCollection.sort({ silent: true });
                    that.ui.nameSpaceSearchTree.jstree(true).refresh();
                }
            });
        },
        getNameSpaceTree: function(options) {
            var that = this,
                nameSpaceList = [],
                allCustomFilter = [],
                namsSpaceTreeData = that.nameSpaceCollection.fullCollection.models,
                openClassificationNodesState = function(treeDate) {
                    if (treeDate.length == 1) {
                        _.each(treeDate, function(model) {
                            model.state["opeaned"] = true;
                        });
                    }
                },
                generateNode = function(nodeOptions, attrNode) {
                    var attributesNode = attrNode ? null : nodeOptions.get("attributeDefs"),
                        nodeStructure = {
                            text: attrNode ? _.escape(nodeOptions.name) : _.escape(nodeOptions.get("name")),
                            name: attrNode ? _.escape(nodeOptions.name) : _.escape(nodeOptions.get("name")),
                            type: "nameSpace",
                            id: attrNode ? _.escape(nodeOptions.name) : nodeOptions.get("guid"),
                            icon: attrNode ? "fa fa-file-o" : "fa fa-folder-o",
                            children: [],
                            state: { selected: nodeOptions.get("guid") === that.namespaceID },
                            gType: "NameSpace",
                            model: nodeOptions
                        };
                    return nodeStructure;
                };
            // getChildren = function(options) {
            //     var children = options.children,
            //         data = [],
            //         dataWithoutEmptyTag = [],
            //         isAttrNode = true;
            //     if (children && children.length) {
            //         _.each(children, function(attrDetail) {
            //             var nodeDetails = {
            //                     name: _.escape(attrDetail.name),
            //                     model: attrDetail
            //                 },
            //                 nodeProperties = {
            //                     parent: options.parent,
            //                     text: _.escape(attrDetail.name),
            //                     model: attrDetail,
            //                     id: options.parent + "_" + _.escape(attrDetail.name)
            //                 },
            //                 getNodeDetails = generateNode(nodeDetails, isAttrNode),
            //                 classificationNode = _.extend(getNodeDetails, nodeProperties);
            //             data.push(classificationNode);
            //         });
            //     } else {
            //         return null;
            //     }
            //     return data;
            // };
            _.each(namsSpaceTreeData, function(filterNode) {
                nameSpaceList.push(generateNode(filterNode));
            });

            var treeView = [{
                icon: "fa fa-folder-o",
                gType: "nameSpace",
                type: "nameSpaceFolder",
                children: nameSpaceList,
                text: "Namespace",
                name: "Namespace",
                state: { opened: true }
            }];
            var customFilterList = treeView;
            return nameSpaceList;
        },
        generateSearchTree: function(options) {
            var $el = options && options.$el,
                type = options && options.type,
                that = this,
                getEntityTreeConfig = function(opt) {
                    return {
                        plugins: ["search", "core", "sort", "conditionalselect", "changed", "wholerow", "node_customize"],
                        conditionalselect: function(node) {
                            var type = node.original.type;
                            if (type == "nameSpaceFolder") {
                                if (node.children.length) {
                                    return false;
                                } else {
                                    return true;
                                }
                            } else {
                                return true;
                            }
                        },
                        state: { opened: true },
                        search: {
                            show_only_matches: true,
                            case_sensitive: false
                        },
                        node_customize: {
                            default: function(el, node) {
                                var aTag = $(el).find(">a.jstree-anchor");
                                aTag.append("<span class='tree-tooltip'>" + aTag.text() + "</span>");
                                if (node.parent === "#") {
                                    $(el).append('<div class="tools"><i class="fa"></i></div>');
                                } else {
                                    $(el).append('<div class="tools"><i class="fa fa-ellipsis-h nameSpacePopover" rel="popover"></i></div>');
                                }
                            }
                        },
                        core: {
                            multiple: false,
                            data: function(node, cb) {
                                if (node.id === "#") {
                                    cb(that.getNameSpaceTree());
                                }
                            }
                        }
                    };
                };
            $el.jstree(
                    getEntityTreeConfig({
                        type: ""
                    })
                )
                .on("open_node.jstree", function(e, data) {
                    that.isTreeOpen = true;
                })
                .on("select_node.jstree", function(e, data) {
                    that.onNodeSelect(data);
                })
                .on("search.jstree", function(nodes, str, res) {
                    if (str.nodes.length === 0) {
                        $el.jstree(true).hide_all();
                        $el.parents(".panel").addClass("hide");
                    } else {
                        $el.parents(".panel").removeClass("hide");
                    }
                })
                .on("hover_node.jstree", function(nodes, str, res) {
                    var aFilter = that.$("#" + str.node.a_attr.id),
                        filterOffset = aFilter.find(">.jstree-icon").offset();
                    that.$(".tree-tooltip").removeClass("show");
                    setTimeout(function() {
                        if (aFilter.hasClass("jstree-hovered") && ($(":hover").last().hasClass("jstree-hovered") || $(":hover").last().parent().hasClass("jstree-hovered")) && filterOffset.top && filterOffset.left) {
                            aFilter
                                .find(">span.tree-tooltip")
                                .css({
                                    top: "calc(" + filterOffset.top + "px - 45px)",
                                    left: "24px"
                                })
                                .addClass("show");
                        }
                    }, 1200);
                })
                .on("dehover_node.jstree", function(nodes, str, res) {
                    that.$(".tree-tooltip").removeClass("show");
                });
        }
    });
    return NameSpaceTreeLayoutView;
});