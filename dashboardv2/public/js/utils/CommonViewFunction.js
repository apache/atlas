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

define(['require', 'utils/Utils', 'modules/Modal'], function(require, Utils, Modal) {
    'use strict';

    var CommonViewFunction = {};
    CommonViewFunction.deleteTagModel = function(tagName) {
        var msg = "<b>Tag:</b>";
        if (tagName) {
            msg = "<b>Tag: " + tagName + "</b>";
        }
        var modal = new Modal({
            title: 'Are you sure you want to delete ?',
            okText: 'Delete',
            htmlContent: msg,
            cancelText: "Cancel",
            allowCancel: true,
            okCloses: true,
            showFooter: true,
        }).open();
        return modal;
    };
    CommonViewFunction.deleteTag = function(options) {
        require(['models/VTag'], function(VTag) {
            var tagModel = new VTag();
            if (options && options.guid && options.tagName)
                tagModel.deleteTag(options.guid, options.tagName, {
                    beforeSend: function() {},
                    success: function(data) {
                        Utils.notifySuccess({
                            content: "Tag " + options.tagName + " has been deleted successfully"
                        });
                        options.collection.fetch({ reset: true });
                    },
                    error: function(error, data, status) {
                        var message = "Tag " + options.tagName + " could not be deleted";
                        if (data.error) {
                            message = data.error;
                        }
                        Utils.notifyError({
                            content: message
                        });
                    },
                    complete: function() {}
                });
        });
    };
    return CommonViewFunction;
});
