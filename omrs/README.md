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

# Open Metadata Repository Service (OMRS)

The Open Metadata Repository Services (OMRS) enable metadata repositories to exchange metadata.
Traditional metadata management technology tends to centralize metadata into a single repository.
An organization often begins with a single metadata repository, typically deployed to support a
single project or initiative.
However, over time, depending on the tools they buy, the projects they run or the political structures
within the organization, the number of deployed metadata repositories grows, creating multiple metadata silos.
So for example, an organization may have:

* a metadata repository and tools for its governance team.
This metadata repository may host the canonical glossary, and the governance policies, rules and classifications.

* a metadata repository for its data lake.
This metadata repository has the details of the data repositories in the data lake and the
movement of data between them.

* a metadata repository for its data integration tools that continuously extract data
from the operational systems and sends them to the data lake.

The role of the OMRS is to bring these metadata repositories together so this metadata can be linked
and used together across the organization.
It enables these metadata repositories to act as a aggregated source of metadata.
The metadata repositories using OMRS may be instances of Apache Atlas and they may
include a mixture of repositories from different vendors that support the OMRS integration interfaces.