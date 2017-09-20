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
    'hbs!tmpl/search/SaveAsLayoutView_tmpl',
    'utils/Utils',
    'modules/Modal',
    'utils/UrlLinks',
    'platform',
    'models/VSearch',
    'utils/CommonViewFunction',
    'utils/Messages'
], function(require, Backbone, SaveAsLayoutViewTmpl, Utils, Modal, UrlLinks, platform, VSearch, CommonViewFunction, Messages) {


    var SaveAsLayoutView = Backbone.Marionette.LayoutView.extend({
        _viewName: 'SaveAsLayoutView',
        template: SaveAsLayoutViewTmpl,
        regions: {},
        ui: {
            saveAsName: "[data-id='saveAsName']"
        },
        events: function() {
            var events = {};
            return events;
        },
        initialize: function(options) {
            var that = this;
            _.extend(this, _.pick(options, 'value', 'collection', 'searchVent', 'typeHeaders', 'fetchFavioriteCollection', 'getValue', 'isBasic'));

            this.model = new VSearch();
            var modal = new Modal({
                title: 'Enter your search name',
                content: this,
                cancelText: "Cancel",
                okCloses: false,
                okText: 'Create',
                allowCancel: true
            }).open();
            modal.$el.find('button.ok').attr("disabled", "true");
            this.ui.saveAsName.on('keyup', function(e) {
                modal.$el.find('button.ok').removeAttr("disabled");
            });
            this.ui.saveAsName.on('keyup', function(e) {
                if ((e.keyCode == 8 || e.keyCode == 32 || e.keyCode == 46) && e.currentTarget.value.trim() == "") {
                    modal.$el.find('button.ok').attr("disabled", "true");
                }
            });
            modal.on('ok', function() {
                modal.$el.find('button.ok').attr("disabled", "true");
                that.onCreateButton(modal);
            });
            modal.on('closeModal', function() {
                modal.trigger('cancel');
            });
        },
        onCreateButton: function(modal) {
            var that = this,
                obj = { value: this.getValue(), name: this.ui.saveAsName.val() },
                saveObj = CommonViewFunction.generateObjectForSaveSearchApi(obj);
            if (this.isBasic) {
                saveObj['searchType'] = "BASIC";
            } else {
                saveObj['searchType'] = "ADVANCED";
            }
            that.model.urlRoot = UrlLinks.saveSearchApiUrl();
            that.model.save(saveObj, {
                success: function(model, data) {
                    if (that.collection) {
                        that.collection.add(data);
                    }
                    Utils.notifySuccess({
                        content: obj.name + Messages.addSuccessMessage
                    });
                }
            });
            modal.trigger('cancel');
        }
    });
    return SaveAsLayoutView;
});