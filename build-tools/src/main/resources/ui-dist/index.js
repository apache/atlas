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

var gatewayUrl;

window.onload = function() {
    const ui = SwaggerUIBundle({
        url: getSwaggerBaseUrl(window.location.pathname) + "/swagger.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [
            SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        requestInterceptor: function(request) {
              if (!request.url.includes("swagger.json")) {
                    request.url = getAPIUrl(request.url);
              }
              request.headers['X-XSRF-HEADER'] = "valid";
              return request;
        },
        docExpansion: 'none',
        validatorUrl: 'none'
    })
    window.ui = ui;

    document.getElementById("swagger-ui").getElementsByClassName("topbar-wrapper")[0].getElementsByTagName("img")[0].src = gatewayUrl + "/img/atlas_logo.svg";
}

function getSwaggerBaseUrl(url) {
    var path = url.replace(/\/[\w-]+.(jsp|html)|\/+$/ig, '');
    splitPath = path.split("/");
    splitPath.pop();
    gatewayUrl = splitPath.join("/");

    return window.location.origin + path;
};

function getAPIUrl(url) {
    url = new URL(url);
    var path =  url.origin + gatewayUrl + url.pathname + url.search;
    return path;
};
