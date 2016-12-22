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
    'hbs!tmpl/tag/addTagModalView_tmpl',
    'collection/VTagList',
    'collection/VCommonList',
    'modules/Modal',
    'models/VEntity',
    'utils/Utils',
    'utils/UrlLinks'
], function(require, AddTagModalViewTmpl, VTagList, VCommonList, Modal, VEntity, Utils, UrlLinks) {
    'use strict';

    var AddTagModel = Marionette.LayoutView.extend({
        template: AddTagModalViewTmpl,

        regions: {},
        ui: {
            addTagOptions: "[data-id='addTagOptions']",
            tagAttribute: "[data-id='tagAttribute']"
        },
        events: function() {
            var events = {};
            events["change " + this.ui.addTagOptions] = 'onChangeTagDefination';
            return events;
        },
        /**
         * intialize a new AddTagModel Layout
         * @constructs
         */
        initialize: function(options) {
            var that = this;
            _.extend(this, _.pick(options, 'vent', 'modalCollection', 'guid', 'callback', 'multiple', 'showLoader'));
            this.collection = new VTagList();
            this.commonCollection = new VTagList();
            this.asyncAttrFetchCounter = 0;
            this.modal = new Modal({
                title: 'Add Tag',
                content: this,
                okText: 'Add',
                cancelText: "Cancel",
                allowCancel: true,
            }).open();
            this.modal.$el.find('button.ok').attr("disabled", true);
            this.on('ok', function() {
                var tagName = this.ui.addTagOptions.val();
                var tagAttributes = {};
                var tagAttributeNames = this.$(".attrName");
                tagAttributeNames.each(function(i, item) {
                    var selection = $(item).data("key");
                    tagAttributes[selection] = $(item).val();
                });

                if (that.multiple) {
                    that.asyncFetchCounter = 0;
                    for (var i = 0; i < that.multiple.length; i++) {
                        if (i == 0) {
                            that.showLoader();
                        }
                        var obj = {
                            tagName: tagName,
                            tagAttributes: tagAttributes,
                            guid: (_.isObject(that.multiple[i].id) ? that.multiple[i].id.id : that.multiple[i].id)
                        }
                        that.saveTagData(obj);
                    }
                } else {
                    that.asyncFetchCounter = 0;
                    that.saveTagData({
                        tagName: tagName,
                        tagAttributes: tagAttributes,
                        guid: that.guid
                    });
                }
            });
            this.on('closeModal', function() {
                this.modal.trigger('cancel');
            });
            this.bindEvents();
        },

        onRender: function() {
            $.extend(this.collection.queryParams, { type: 'TRAIT', notsupertype: 'TaxonomyTerm' });
            this.collection.fetch({ reset: true });
        },
        bindEvents: function() {
            this.listenTo(this.collection, 'reset', function() {
                this.tagsCollection();
            }, this);
            this.listenTo(this.commonCollection, 'reset', function() {
                --this.asyncAttrFetchCounter
                this.subAttributeData();
            }, this);
            this.listenTo(this.commonCollection, 'error', function() {
                --this.asyncAttrFetchCounter
                this.$('.attrLoader').hide();
            }, this);
        },
        tagsCollection: function() {
            this.collection.fullCollection.comparator = function(model) {
                return model.get('name').toLowerCase();
            }

            var str = '<option selected="selected" disabled="disabled">-- Select a tag from the dropdown list --</option>';
            this.collection.fullCollection.sort().each(function(obj, key) {
                str += '<option>' + _.escape(obj.get('name')) + '</option>';
            });
            this.ui.addTagOptions.html(str);
            this.ui.addTagOptions.select2({
                placeholder: "Select Tag",
                allowClear: false
            });
        },
        onChangeTagDefination: function() {
            this.ui.tagAttribute.empty();
            var saveBtn = this.modal.$el.find('button.ok');
            saveBtn.prop("disabled", false);
            var tagname = this.ui.addTagOptions.val();
            this.hideAttributeBox();
            this.fetchTagSubData(tagname);
        },
        fetchTagSubData: function(tagname) {
            this.commonCollection.url = UrlLinks.typesClassicationApiUrl(tagname);
            ++this.asyncAttrFetchCounter
            this.commonCollection.fetch({ reset: true });
        },
        showAttributeBox: function() {
            if (this.asyncAttrFetchCounter === 0) {
                this.$('.attrLoader').hide();
                if (this.ui.tagAttribute.children().length !== 0) {
                    this.ui.tagAttribute.parent().show();
                }
            }
        },
        hideAttributeBox: function() {
            this.ui.tagAttribute.children().empty();
            this.ui.tagAttribute.parent().hide();
            this.$('.attrLoader').show();
        },
        subAttributeData: function() {
            var that = this;
            if (this.commonCollection.models[0]) {
                if (this.commonCollection.models[0].get('attributeDefs')) {
                    _.each(this.commonCollection.models[0].get('attributeDefs'), function(obj) {
                        that.ui.tagAttribute.append('<div class="form-group"><label>' + _.escape(obj.name) + '</label>' +
                            '<input type="text" class="form-control attributeInputVal attrName" data-key="' + obj.name + '" ></input></div>');
                    });
                }

                if (this.commonCollection.models[0].get('superTypes')) {
                    var superTypes = this.commonCollection.models[0].get('superTypes');
                    if (!_.isArray(superTypes)) {
                        superTypes = [superTypes];
                    }
                    if (superTypes.length) {
                        _.each(superTypes, function(name) {
                            that.fetchTagSubData(name);
                        });
                    } else {
                        this.showAttributeBox();
                    }

                } else {
                    this.showAttributeBox();
                }
            } else {
                this.showAttributeBox();
            }
        },
        saveTagData: function(options) {
            var that = this;
            ++this.asyncFetchCounter;
            this.entityModel = new VEntity();
            var tagName = options.tagName,
                tagAttributes = options.tagAttributes,
                json = [{
                    "typeName": tagName,
                    "attributes": tagAttributes
                }];
            this.entityModel.saveEntity(options.guid, {
                data: JSON.stringify(json),
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Tag " + tagName + " has been added to entity"
                    });
                    if (options.modalCollection) {
                        options.modalCollection.fetch({ reset: true });
                    }
                },
                error: function(error, data, status) {
                    var message = "Tag " + tagName + " could not be added";
                    if (error && error.responseText) {
                        var data = JSON.parse(error.responseText);
                        message = data.error;
                    }
                    Utils.notifyError({
                        content: message
                    });
                },
                complete: function() {
                    --that.asyncFetchCounter;
                    if (that.callback && that.asyncFetchCounter === 0) {
                        that.callback();
                    }
                }
            });
        },
    });
    return AddTagModel;
});
