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
    'hbs!tmpl/glossary/TermRelationAttributeLayoutView_tmpl',
    'hbs!tmpl/glossary/TermRelationAttributeTable_tmpl',
    'utils/Enums',
    'utils/Utils',
    'utils/UrlLinks'
], function(require, Backbone, TermRelationAttributeLayoutViewTmpl, TermRelationAttributeTableTmpl, Enums, Utils, UrlLinks) {

    var TermRelationAttributeLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TermRelationAttributeLayoutView */
        {
            _viewName: 'TermRelationAttributeLayoutView',

            template: TermRelationAttributeLayoutViewTmpl,

            templateHelpers: function() {
                return {
                    attributeList: Enums.termRelationAttributeList
                };
            },

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                "termAttributeSelect": '[data-id="termAttributeSelect"]',
                "addTermRelation": '[data-id="addTermRelation"]',
                "termAttributeTable": '[data-id="termAttributeTable"]',
                "deleteAttribute": '[data-id="deleteAttribute"]',
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTermRelation] = 'onAddTermRelation';
                events["click " + this.ui.deleteAttribute] = 'onDeleteAttribute';
                events["change " + this.ui.termAttributeSelect] = 'changeTermAttributeSelect';
                return events;
            },
            /**
             * intialize a new TermRelationAttributeLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'glossaryCollection', 'data', 'fetchCollection', 'getSelectedTermAttribute', 'setSelectedTermAttribute'));
                this.selectedTermAttribute = this.getSelectedTermAttribute();
            },
            bindEvents: function() {},
            onAddTermRelation: function(e) {
                var that = this;
                this.selectedTermAttribute = this.ui.termAttributeSelect.val();
                this.setSelectedTermAttribute(this.selectedTermAttribute);
                require(['views/glossary/AssignTermLayoutView'], function(AssignTermLayoutView) {
                    var view = new AssignTermLayoutView({
                        "isAttributeRelationView": true,
                        "termData": that.data,
                        "selectedTermAttribute": that.selectedTermAttribute,
                        "callback": function() {
                            if (that.fetchCollection) {
                                that.fetchCollection();
                            }
                        },
                        "glossaryCollection": that.glossaryCollection
                    });
                    view.modal.on('ok', function() {
                        //that.hideLoader();
                    });
                });
            },
            onDeleteAttribute: function(e) {
                var that = this,
                    notifyObj = {
                        modal: true,
                        text: "Are you sure you want to remove term association",
                        ok: function(argument) {
                            var model = new that.glossaryCollection.model(),
                                selectedGuid = $(e.currentTarget).data('termguid'),
                                ajaxOptions = {
                                    success: function(rModel, response) {
                                        Utils.notifySuccess({
                                            content: "Association removed successfully "
                                        });
                                        if (that.fetchCollection) {
                                            that.fetchCollection();
                                        }
                                    }
                                },
                                data = _.clone(that.data);
                            data[that.selectedTermAttribute] = _.reject(data[that.selectedTermAttribute], function(obj) {
                                return obj.termGuid == selectedGuid;
                            });
                            model.removeTermFromAttributes(_.extend(ajaxOptions, { data: JSON.stringify(data) }, { guid: that.data.guid }));

                        },
                        cancel: function(argument) {}
                    };
                Utils.notifyConfirm(notifyObj);
            },
            changeTermAttributeSelect: function(e, options) {
                var $el = $(e.currentTarget);
                if (e.type == "change" && $el.select2('data')) {
                    this.selectedTermAttribute = $el.val();
                    this.setSelectedTermAttribute(this.selectedTermAttribute);
                    this.ui.termAttributeTable.html(TermRelationAttributeTableTmpl({
                        attributeValue: this.data[this.selectedTermAttribute],
                        attributes: Enums.termRelationAttributeList[this.selectedTermAttribute]
                    }))
                }
            },
            onRender: function() {
                if (this.selectedTermAttribute) {
                    this.ui.termAttributeSelect.val(this.selectedTermAttribute);
                }
                this.ui.termAttributeSelect.select2().trigger('change');
            }
        });
    return TermRelationAttributeLayoutView;
});