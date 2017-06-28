/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var $ = require("jquery");
var ODFGlobals = require("./odf-globals.js");

var ConfigurationStore = {

  // readUserDefinedProperties(successCallback, alertCallback) {
   readConfig(successCallback, alertCallback) {
	   if (alertCallback) {
	     alertCallback({type: ""});
	   }
     // clear alert

     return $.ajax({
       url: ODFGlobals.apiPrefix + "settings",
       dataType: 'json',
       type: 'GET',
       success: successCallback,
       error: function(xhr, status, err) {
         if (alertCallback) {
            var msg = "Error while reading user defined properties: " + err.toString();
            alertCallback({type: "danger", message: msg});
         }
       }
      }).abort;
   },

   updateConfig(config, successCallback, alertCallback) {
		if (alertCallback) {
			 alertCallback({type: ""});
		}

	    return $.ajax({
		       url: ODFGlobals.apiPrefix + "settings",
		       contentType: "application/json",
		       dataType: 'json',
		       type: 'PUT',
		       data: JSON.stringify(config),
		       success: successCallback,
		       error: function(xhr, status, err) {
		         if (alertCallback) {
		            var msg = "Error while reading user defined properties: " + err.toString();
		            alertCallback({type: "danger", message: msg});
		         }
		       }
	     }).abort;
   }
}

module.exports = ConfigurationStore;
