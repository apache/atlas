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
    'utils/Utils'
], function(require, AddTagModalViewTmpl, VTagList, VCommonList, Modal, VEntity, Utils) {
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
            _.extend(this, _.pick(options, 'vent', 'modalCollection', 'guid', 'callback'));
            this.collection = new VTagList();
            this.commonCollection = new VCommonList();
            this.modal = new Modal({
                title: 'Add Tag',
                content: this,
                okText: 'Save',
                cancelText: "Cancel",
                allowCancel: true,
            }).open();
            // var saveBtn = this.modal.$el.find('.btn-atlas');
            // saveBtn[0].setAttribute('disabled', true);
            this.on('ok', function() {
                that.saveTagData();
            });
            this.on('closeModal', function() {
                this.modal.trigger('cancel');
            });
            this.bindEvents();
        },

        onRender: function() {
            $.extend(this.collection.queryParams, { type: 'TRAIT' });
            this.collection.fetch({ reset: true });
        },
        bindEvents: function() {
            this.listenTo(this.collection, 'reset', function() {
                this.tagsCollection();
            }, this);
            this.listenTo(this.commonCollection, 'reset', function() {
                this.subAttributeData();
            }, this);
        },
        tagsCollection: function() {
            var str = '<option selected="selected" disabled="disabled">-- Select Tag --</option>';
            for (var i = 0; i < this.collection.fullCollection.models.length; i++) {
                var tags = this.collection.fullCollection.models[i].get("tags");
                str += '<option>' + tags + '</option>';
                this.ui.addTagOptions.html(str);
            }
        },
        onChangeTagDefination: function() {
            this.ui.tagAttribute.empty();
            var saveBtn = this.modal.$el.find('.btn-success');
            saveBtn.prop("disabled", false);
            var tagname = this.ui.addTagOptions.val();
            this.fetchTagSubData(tagname);
        },
        fetchTagSubData: function(tagname) {
            this.commonCollection.url = "/api/atlas/types/" + tagname;
            this.commonCollection.modelAttrName = 'definition';
            this.commonCollection.fetch({ reset: true });
        },
        subAttributeData: function() {
            if (this.commonCollection.models[0] && this.commonCollection.models[0].attributes && this.commonCollection.models[0].attributes.traitTypes[0].attributeDefinitions) {
                for (var i = 0; i < this.commonCollection.models[0].attributes.traitTypes[0].attributeDefinitions.length; i++) {
                    var attribute = this.commonCollection.models[0].attributes.traitTypes[0].attributeDefinitions;
                    this.ui.tagAttribute.show();
                    this.strAttribute = '<label class="control-label col-sm-4 ng-binding">' + attribute[i].name + '</label>' +
                        '<div class="col-sm-8 input-spacing">' +
                        '<input type="text" class="form-control attributeInputVal attrName" data-key="' + attribute[i].name + '" ></input></div>';
                    this.ui.tagAttribute.append(this.strAttribute);
                }
                if (this.commonCollection.models[0].attributes.traitTypes[0].superTypes.length > 0) {
                    for (var j = 0; j < this.commonCollection.models[0].attributes.traitTypes[0].superTypes.length; j++) {
                        var superTypeAttr = this.commonCollection.models[0].attributes.traitTypes[0].superTypes[j];
                        this.fetchTagSubData(superTypeAttr);
                    }
                }
            }
        },
        saveTagData: function() {
            var that = this,
                values = {};
            this.entityModel = new VEntity();
            var names = this.$(".attrName");
            names.each(function(i, item) {
                var selection = $(item).data("key");
                values[selection] = $(item).val();
            });
            var tagName = this.ui.addTagOptions.val();
            var json = {
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                "typeName": tagName,
                "values": values
            };
            that.entityModel.saveEntity(that.guid, {
                data: JSON.stringify(json),
                beforeSend: function() {},
                success: function(data) {
                    Utils.notifySuccess({
                        content: "Tag " + tagName + " has been added to entity"
                    });
                    if (that.callback) {
                        that.callback();
                    }
                    if (that.modalCollection) {
                        that.modalCollection.fetch({ reset: true });
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
                complete: function() {}
            });
        },
    });
    return AddTagModel;
});
