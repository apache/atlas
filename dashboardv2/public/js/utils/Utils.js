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

define(['require', 'utils/Globals', 'pnotify', 'utils/Messages', 'pnotify.buttons', 'pnotify.confirm'], function(require, Globals, pnotify, Messages) {
    'use strict';

    var Utils = {};
    var prevNetworkErrorTime = 0;

    Utils.escapeHtml = function(string) {
        var entityMap = {
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': '&quot;',
            "'": '&#39;',
            "/": '&#x2F;'
        };
        return String(string).replace(/[&<>"'\/]/g, function(s) {
            return entityMap[s];
        });
    }
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

    var notify = function(options) {
        return new pnotify(_.extend({ icon: true, hide: true, delay: 3000, remove: true }, options));
    }
    Utils.notifyInfo = function(options) {
        notify({
            type: "info",
            text: (options.html ? options.content : _.escape(options.content)) || "Info message."
        });
    };

    Utils.notifyWarn = function(options) {
        notify({
            type: "notice",
            text: (options.html ? options.content : _.escape(options.content)) || "Info message."
        });
    };

    Utils.notifyError = function(options) {
        notify({
            type: "error",
            text: (options.html ? options.content : _.escape(options.content)) || "Error occurred."
        });
    };

    Utils.notifySuccess = function(options) {
        notify({
            type: "success",
            text: (options.html ? options.content : _.escape(options.content)) || "Error occurred."
        });
    };

    Utils.notifyConfirm = function(options) {
        notify(_.extend({
            title: 'Confirmation',
            hide: false,
            confirm: {
                confirm: true
            },
            buttons: {
                closer: false,
                sticker: false
            },
            history: {
                history: false
            }
        }, options)).get().on('pnotify.confirm', function() {
            $('.ui-pnotify-modal-overlay').remove().fadeOut();
            if (options.ok) {
                options.ok();
            }
        }).on('pnotify.cancel', function() {
            $('.ui-pnotify-modal-overlay').remove().fadeOut();
            if (options.cancel) {
                options.cancel();
            }
        });
    }
    Utils.defaultErrorHandler = function(model, error) {
        if (error && error.status) {
            if (error.status == 401) {
                window.location = 'login.jsp'
            } else if (error.status == 419) {
                window.location = 'login.jsp'
            } else if (error.status == 403) {
                var message = "You are not authorized";
                if (error.statusText) {
                    try {
                        message = JSON.parse(error.statusText).AuthorizationError;
                    } catch (err) {}
                    Utils.notifyError({
                        content: message
                    });
                }
            } else if (error.status == "0" && error.statusText != "abort") {
                var diffTime = (new Date().getTime() - prevNetworkErrorTime);
                if (diffTime > 3000) {
                    prevNetworkErrorTime = new Date().getTime();
                    Utils.notifyError({
                        content: "Network Connection Failure : " +
                            "It seems you are not connected to the internet. Please check your internet connection and try again"
                    });
                }
            } else {
                Utils.serverErrorHandler(model, error)
            }
        } else {
            Utils.serverErrorHandler(model, error)
        }
    };
    Utils.serverErrorHandler = function(model, response) {
        var responseJSON = response ? response.responseJSON : response;
        if (response && responseJSON && (responseJSON.errorMessage || responseJSON.message || responseJSON.error)) {
            Utils.notifyError({
                content: responseJSON.errorMessage || responseJSON.message || responseJSON.error
            });
        } else {
            Utils.notifyError({
                content: Messages.defaultErrorMessage
            });
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
            var keyValue = localStorage.getItem(key);
            if (!keyValue || keyValue == "undefined") {
                return this.setLocalStorage(key, value);
            } else {
                return { found: true, 'value': keyValue };
            }
        }
    };
    Utils.cookie = {
        setCookie: function(cname, cvalue) {
            //var d = new Date();
            //d.setTime(d.getTime() + (exdays*24*60*60*1000));
            //var expires = "expires=" + d.toGMTString();
            document.cookie = cname + "=" + cvalue + "; ";
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
    };

    Utils.setUrl = function(options) {
        if (options) {
            if (options.mergeBrowserUrl) {
                var param = Utils.getUrlState.getQueryParams();
                if (param) {
                    options.urlParams = $.extend(param, options.urlParams);
                }
            }
            if (options.urlParams) {
                var urlParams = "?";
                _.each(options.urlParams, function(value, key, obj) {
                    if (value != undefined || value != null) {
                        value = String(value);
                    }
                    value = value || null;
                    if (value) {
                        urlParams += key + "=" + value + "&";
                    }
                });
                urlParams = urlParams.slice(0, -1);
                options.url += urlParams;
            }
            if (options.updateTabState) {
                $.extend(Globals.saveApplicationState.tabState, options.updateTabState());
            }
            Backbone.history.navigate(options.url, { trigger: options.trigger != undefined ? options.trigger : true });
        }
    };

    Utils.getUrlState = {
        getQueryUrl: function(url) {
            var hashValue = window.location.hash;
            if (url) {
                hashValue = url;
            }
            return {
                firstValue: hashValue.split('/')[1],
                hash: hashValue,
                queyParams: hashValue.split("?"),
                lastValue: hashValue.split('/')[hashValue.split('/').length - 1]
            }
        },
        isInitial: function() {
            return this.getQueryUrl().firstValue == undefined ? true : false;
        },
        isTagTab: function() {
            return this.getQueryUrl().firstValue == "tag" ? true : false;
        },
        isTaxonomyTab: function() {
            return this.getQueryUrl().firstValue == "taxonomy" ? true : false;
        },
        isSearchTab: function() {
            return this.getQueryUrl().firstValue == "search" ? true : false;
        },
        isDetailPage: function() {
            return this.getQueryUrl().firstValue == "detailPage" ? true : false;
        },
        getLastValue: function() {
            return this.getQueryUrl().lastValue;
        },
        getFirstValue: function() {
            return this.getQueryUrl().firstValue;
        },
        getQueryParams: function(url) {
            var qs = this.getQueryUrl(url).queyParams[1];
            if (typeof qs == "string") {
                qs = qs.split('+').join(' ');
                var params = {},
                    tokens,
                    re = /[?&]?([^=]+)=([^&]*)/g;
                while (tokens = re.exec(qs)) {
                    params[decodeURIComponent(tokens[1])] = decodeURIComponent(tokens[2]);
                }
                return params;
            }
        },
        getKeyValue: function(key) {
            var paramsObj = this.getQueryParams();
            if (key.length) {
                var values = [];
                _.each(key, function(objKey) {
                    var obj = {};
                    obj[objKey] = paramsObj[objKey]
                    values.push(obj);
                    return values;
                })
            } else {
                return paramsObj[key];
            }
        }
    }
    Utils.checkTagOrTerm = function(value, isTermView) {
        if (value && _.isString(value) && isTermView) {
            // For string break
            if (value == "TaxonomyTerm") {
                return {}
            }
            var name = _.escape(value).split('.');
            return {
                term: true,
                tag: false,
                name: name[name.length - 1],
                fullName: value
            }
        }
        if (value && _.isString(value)) {
            value = {
                typeName: value
            }
        }
        if (_.isObject(value)) {
            var name = "";
            if (value && value.$typeName$) {
                name = value.$typeName$;
            } else if (value && value.typeName) {
                name = value.typeName;
            }
            if (name === "TaxonomyTerm") {
                return {}
            }
            name = _.escape(name).split('.');

            var trem = false;
            if (value['taxonomy.namespace']) {
                trem = true;
            } else if (value.values && value.values['taxonomy.namespace']) {
                trem = true;
            } else if (name.length > 1) {
                trem = true; // Temp fix
            }

            if (trem) {
                return {
                    term: true,
                    tag: false,
                    name: name[name.length - 1],
                    fullName: name.join('.')
                }
            } else {
                return {
                    term: false,
                    tag: true,
                    name: name[name.length - 1],
                    fullName: name.join('.')
                }
            }
        }
    }
    Utils.getName = function(collectionJSON, priorityAttribute) {
        if (collectionJSON) {
            if (collectionJSON.attributes && collectionJSON.attributes[priorityAttribute]) {
                return _.escape(collectionJSON.attributes[priorityAttribute]);
            }
            if (collectionJSON[priorityAttribute]) {
                return _.escape(collectionJSON[priorityAttribute]);
            }
            if (collectionJSON.attributes) {
                if (collectionJSON.attributes.name) {
                    return _.escape(collectionJSON.attributes.name);
                }
                if (collectionJSON.attributes.qualifiedName) {
                    return _.escape(collectionJSON.attributes.qualifiedName);
                }
                if (collectionJSON.attributes.id) {
                    return _.escape(collectionJSON.attributes.id);
                }
            }
            if (collectionJSON.name) {
                return _.escape(collectionJSON.name);
            }
            if (collectionJSON.qualifiedName) {
                return _.escape(collectionJSON.qualifiedName);
            }
            if (collectionJSON.displayText) {
                return _.escape(collectionJSON.displayText);
            }
            if (collectionJSON.guid) {
                return _.escape(collectionJSON.guid);
            }
            if (collectionJSON.id) {
                return _.escape(collectionJSON.id);
            }
        }
        return "-";
    }
    Utils.showTitleLoader = function(loaderEl, titleBoxEl) {
        loaderEl.css({
            'display': 'block',
            'position': 'relative',
            'height': '85px',
            'marginTop': '85px',
            'marginLeft': '50%',
            'left': '0%'
        });
        titleBoxEl.hide();
    }
    Utils.hideTitleLoader = function(loaderEl, titleBoxEl) {
        loaderEl.hide();
        titleBoxEl.fadeIn();
    }

    $.fn.toggleAttribute = function(attributeName, firstString, secondString) {
        if (this.attr(attributeName) == firstString) {
            this.attr(attributeName, secondString);
        } else {
            this.attr(attributeName, firstString);
        }
    }
    $('body').on('click', '.expand_collapse_panel', function() {
        var icon = $(this).find('i'),
            panel = $(this).parents('.panel'),
            panelBody = panel.find('.panel-body');
        icon.toggleClass('fa-chevron-up fa-chevron-down');
        $(this).toggleAttribute('title', 'Collapse', 'Expand');
        panelBody.toggle('0.5', 'linear');
        $(this).trigger('expand_collapse_panel', [$(this).parents('.panel')]);
    });
    $('body').on('click', '.fullscreen_panel', function() {
        var icon = $(this).find('i'),
            panel = $(this).parents('.panel'),
            panelBody = panel.find('.panel-body');
        icon.toggleClass('fa-expand fa-compress');
        $(this).toggleAttribute('title', 'Fullscreen', 'Exit Fullscreen');
        panel.toggleClass('panel-fullscreen');
        panel.find('.expand_collapse_panel').toggle();
        // Condition if user clicks on fullscree button and body is in collapse mode.
        if (panel.hasClass('panel-fullscreen')) {
            $('body').css("position", "fixed");
            if (!panelBody.is(':visible')) {
                panelBody.show();
                panelBody.addClass('full-visible');
            }
            //first show body to get width and height for postion then trigger the event.
            $(this).trigger('fullscreen_done', [$(this).parents('.panel')]);
        } else if (panelBody.hasClass('full-visible')) {
            $('body').removeAttr("style");
            $(this).trigger('fullscreen_done', [$(this).parents('.panel')]);
            //first trigger the event to getwidth and height for postion then hide body.
            panelBody.hide();
            panelBody.removeClass('full-visible');
        } else {
            $('body').removeAttr("style");
            $(this).trigger('fullscreen_done', [$(this).parents('.panel')]);
        }


    });
    return Utils;
});
