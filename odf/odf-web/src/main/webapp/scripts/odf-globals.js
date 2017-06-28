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
const CONTEXT_ROOT = ""; // window.location.origin + "/" + (window.location.pathname.split("/")[1].length > 0 ? window.location.pathname.split("/")[1] + "/" : "");
const API_PREFIX = CONTEXT_ROOT + API_PATH;
const SERVICES_URL = API_PREFIX + "services";
const ANALYSIS_URL = API_PREFIX + "analyses";
const ENGINE_URL = API_PREFIX + "engine";
const CONFIG_URL = API_PREFIX + "config";
const METADATA_URL = API_PREFIX + "metadata";
const IMPORT_URL = API_PREFIX + "import";
const ANNOTATIONS_URL = API_PREFIX + "annotations";

var OdfUrls = {
	"contextRoot": CONTEXT_ROOT,
	"apiPrefix": API_PREFIX,
	"servicesUrl": SERVICES_URL,
	"analysisUrl": ANALYSIS_URL,
	"engineUrl": ENGINE_URL,
	"configUrl": CONFIG_URL,
	"metadataUrl": METADATA_URL,
	"importUrl": IMPORT_URL,
	"annotationsUrl": ANNOTATIONS_URL,

	getPathValue: function(obj, path) {
	    var value = obj;
        $.each(path.split("."),
            function(propKey, prop) {
               // if value is null, do nothing
               if (value) {
                   if(value[prop] != null && value[prop] != undefined){
                       value = value[prop];
                   } else {
                       value = null;
                   }
               }
           }
        );
        return value;
	}
};

module.exports = OdfUrls;
