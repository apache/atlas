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

 define(['require', 'utils/Globals'], function(require, Globals) {
    'use strict';

    var Utils = {};
    require(['noty'], function() {
        $.extend($.noty.defaults, {
            timeout: 5000,
            layout: "topRight",
            theme: "relax",
            closeWith: ['click', 'button'],
            animation: {
                open: 'animated flipInX',
                close: 'animated flipOutX',
                easing: 'swing',
                speed: 500
            }
        });
    });
    Utils.generateUUID = function() {
        var d = new Date().getTime();
        if (window.performance && typeof window.performance.now === "function") {
            d += performance.now(); //use high-precision timer if available
        }
        var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = (d + Math.random() * 16) % 16 | 0;
            d = Math.floor(d / 16);
            return (c == 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
        return uuid;
    };

    Utils.notifyInfo = function(options) {
        noty({
            type: "information",
            text: "<i class='fa fa-exclamation-circle'></i> " + (options.content || "Info message.")
        });
    };
    Utils.notifyWarn = function(options) {
        noty({
            type: "warning",
            text: "<i class='fa fa-times-circle'></i> " + (options.content || "Info message.")
        });
    };

    Utils.notifyError = function(options) {
        noty({
            type: "error",
            text: "<i class='fa fa-times-circle'></i> " + (options.content || "Error occurred.")
        });
    };

    Utils.notifySuccess = function(options) {
        noty({
            type: "success",
            text: "<i class='fa fa-check-circle-o'></i> " + (options.content || "Error occurred.")
        });
    };
    Utils.defaultErrorHandler = function(model, error) {
        if (error.status == 401) {
             window.location = '/login.jsp'
        } else if (error.status == 419) {
             window.location = '/login.jsp'
        }

    };

    Utils.localStorage = {
        checkLocalStorage: function(key, value) {
            if (typeof(Storage) !== "undefined") {
                return this.getLocalStorage(key, value);
            } else {
                console.log('Sorry! No Web Storage support');
                Utils.cookie.checkCookie(key, value);
            }
        },
        setLocalStorage: function(key, value) {
            localStorage.setItem(key, value);
            return { found: false, 'value': value };
        },
        getLocalStorage: function(key, value) {
            var keyValue = localStorage.getItem(key)
            if (!keyValue || keyValue == "undefined") {
                return this.setLocalStorage(key, value);
            } else {
                return { found: true, 'value': keyValue };
            }
        }
    }
    Utils.cookie = {
        setCookie: function(cname, cvalue) {
            //var d = new Date();
            //d.setTime(d.getTime() + (exdays*24*60*60*1000));
            //var expires = "expires=" + d.toGMTString();
            document.cookie = cname + "=" + cvalue + "; "
            return { found: false, 'value': cvalue };
        },
        getCookie: function(findString) {
            var search = findString + "=";
            var ca = document.cookie.split(';');
            for (var i = 0; i < ca.length; i++) {
                var c = ca[i];
                while (c.charAt(0) == ' ') c = c.substring(1);
                if (c.indexOf(name) == 0) {
                    return c.substring(name.length, c.length);
                }
            }
            return "";
        },
        checkCookie: function(key, value) {
            var findString = getCookie(key);
            if (findString != "" || keyValue != "undefined") {
                return { found: true, 'value': ((findString == "undefined") ? (undefined) : (findString)) };
            } else {
                return setCookie(key, value);
            }
        }
    }
    Utils.getQueryParams = function(qs) {
        qs = qs.split('+').join(' ');
        var params = {},
            tokens,
            re = /[?&]?([^=]+)=([^&]*)/g;
        while (tokens = re.exec(qs)) {
            params[decodeURIComponent(tokens[1])] = decodeURIComponent(tokens[2]);
        }
        return params;
    }

    Utils.setUrl = function(options) {
        if (options) {
            if (options.mergeBrowserUrl) {
                var hashUrl = window.location.hash.split("?");
                if (hashUrl.length > 1) {
                    var param = Utils.getQueryParams(hashUrl[1]);
                    options.urlParams = _.extend(param, options.urlParams)
                }
            }
            if (options.urlParams) {
                var urlParams = "?"
                _.each(options.urlParams, function(value, key, obj) {
                    urlParams += key + "=" + value + "&";
                });
                urlParams = urlParams.slice(0, -1);
                options.url += urlParams;
            }
            Backbone.history.navigate(options.url, { trigger: options.trigger != undefined ? options.trigger : true });
        }
    }
    return Utils;
});