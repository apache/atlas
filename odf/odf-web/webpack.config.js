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
var path = require('path');

const APP_ROOT="./src/main/webapp";
const MAIN_FILE= path.resolve(APP_ROOT + "/scripts/odf-console.js");
const CLIENT_FILE= path.resolve(APP_ROOT + "/scripts/odf-client.js");

module.exports = {
	entry: {
		"odf-web": MAIN_FILE,
		"odf-client": CLIENT_FILE
	},

    output: {
        filename: "/[name].js",
        path: path.resolve(APP_ROOT)
    },

    module: {
	    loaders: [
	      {
	        test: /\.jsx?$/,
	        loader: 'babel',
	        query: {
	            presets: ['react', 'es2015']
	        },
	        	include: /(webapp)/,
	        	exlude: /(odf-web.js)/
	      },
	      {
	    	  test: /\.(jsx|js)$/,
	    	  loader: 'imports?jQuery=jquery,$=jquery,this=>window'
	      },
	      {
	          test: /\.css$/,
	          loader: 'style!css'
	      },
	      {
	          test: /\.(png|jpg)$/,
	          loader: 'url?limit=25000&name=resources/img/[hash].[ext]'
	      },
	      {
	    	  test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        	  loader: 'url-loader?limit=25000&&minetype=application/font-woff&name=resources/fonts/[hash].[ext]'
          },
          {
        	  test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
	          loader: 'url?limit=25000&name=resources/fonts/[hash].[ext]'
          }
	    ]
    }
}
