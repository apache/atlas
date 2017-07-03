<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# First Steps

This section assumes that you have ODF installed, either the
[test environment](test-env.html) or [manually](install.html).


### ODF Console UI

To open the ODF console point your browser to the ODF web application.
In the [test environment](test-env.html), this is typically
[https://localhost:58081/odf-web-0.1.0-SNAPSHOT](https://localhost:58081/odf-web-0.1.0-SNAPSHOT).

*Note*: The links to the ODF Console in the instructions below only work if you view this documentation
from the ODF web application.

The default user of the ODF Console is odf / admin4odf.


#### Check System health
Go to the [System monitor](/odf-web-0.1.0-SNAPSHOT/#monitor) tab and
click "Check health". After a while you should see a message
that the health check was successful. If this check fails it might be the case that the dependent services
are not fully up and running yet (typically happens after start of the test environment), so wait a short while
and try again.

#### Discovery Services
Take a look at all available discovery services on the tab [Discovery Services](/odf-web-0.1.0-SNAPSHOT/#discoveryServices).

#### Configure Atlas repository and create sample metadata

To change the URL of your Atlas installation and to create some sample data, go
to the [Configuration](/odf-web-0.1.0-SNAPSHOT/#configuration) tab.
In general, it is a good idea change the default URL to Atlas from "localhost" to a hostname that is accessible
from your network. If you don't do this, you might experience some strange effects when viewing
Atlas annotations from the web app.
If you changed the name, click Save.

Create a set of simple sample data by clicking on Create Atlas Sample Data.

To explore the sample data go to the [Data Sets](/odf-web-0.1.0-SNAPSHOT/#data) tab.

#### Run analysis

The easiest way to start an analysis is from the [Data Sets](/odf-web-0.1.0-SNAPSHOT/#data) tab.
In the "Data Files" section look for the sample table "BankClientsShort".
To view the details of the table click on it anywhere in the row. The Details dialog
shows you information about this data set. Click Close to close the dialog.

To start an analysis on the "BankClientsShort" table click "Start Analysis" on the right.
In the "New Analysis Request" dialog click on "&lt;Select a Service&gt;" to add a service to
the sequence of discovery service to run on the data set. Then click "Submit" to start the analysis.

To check the status of your request go to the
[Analysis](/odf-web-0.1.0-SNAPSHOT/#analysis) tab and click Refresh.
If all went well the status is "Finished".
Click on "View Results" to view all annotations created for this analysis request.


### REST API
See the [REST API documentation](/odf-web-0.1.0-SNAPSHOT/swagger) for more details on how to
perform the actions explained above with the REST API.
In particular, have a look at the ``analysis`` REST resource for APIs how to start and
monitor analyis requests.
