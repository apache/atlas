#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# ODF Jenkins build

## General
The Jenkins build is set up at
[https://shared-discovery-platform-jenkins.swg-devops.com:8443](https://shared-discovery-platform-jenkins.swg-devops.com:8443).

### Available jobs
The following jobs are available:

1. **Open-Discovery-Framework**: The main build on the master branch with the test environment.
Built on Linux.
2. **Open-Discovery-Framework-Parameters**: Job you can trigger manually for private branches
and platforms. Using the `nodelabel` parameter with value `odfbuild` triggers the build on Linux.
3. **Open-Discovery-Framework-Testenv**: Manages and/or installs the test env. This job is currently scheduled
to install the current testenv on the [machine associated with label `odftestenv`](http://sdp1.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT) every night at 10PM EST.

The parameter `nodelabel` defines the nodes the test env is / should be installed:

- `odftestenv` for testing your private builds on [https://sdp1.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT](https://sdp1.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT).
- `odfdemo` for the stable demo on [https://odfdemo.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT](https://odfdemo.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT).

Possible actions selectable through the `action` parameter are:

- `start`: (re)start the test env
- `stop`: stop the test env
- `cleanconfig`: (re)starts with clean configuration and Kafka topics
- `cleanmetadata`: (re)starts with clean metadata
- `cleanall`: (re)starts with cleanconfig plus cleanmetadata
- `install`: Installs the build as specified in the `jenkinsjob` and `buildnumber` parameters.

4. **Open-Discovery-Framework-BuildStarter**: Job polling for changes in the master branch and triggering
the automated build. Starts the Open-Discovery-Framework Linux build. *You should typically not have to trigger this job manually!*

You can find these jobs in Jenkins in the [1-ODF](https://shared-discovery-platform-jenkins.swg-devops.com:8443/view/1-ODF/) tab.

### Node labels
This Jenkins system currently contains two kinds of slaves which are distinguished by a
so called [node label](https://www.safaribooksonline.com/library/view/jenkins-the-definitive/9781449311155/ch11s04.html).

We currently have these node labels:

1. `odfbuild`: Linux build
2. `odftestenv`: Machine sdp1.rtp.raleigh.ibm.com where test envs can be deployed regularly for internal testing.


### Some Important Settings

- Use the profile `jenkinsbuild`. This is currently only used in the Bluemix Services and requires that the Bluemix password is not read from the `cf.password` system property but rather from the env var `CFPASSWORD`. This is only done so that the password doesn't appear in the log.
- The firewall is smashed with a script called `smashlittletonfirewall.sh` (see below). You have to set the env var
`INTRANETCREDENTIALS` from Jenkins as a combined credential variable (of the form user:password). The reason
why this is a script and not put into the command line directly is that the user / password don't appear in the log


### Build Slave Machines
The build slave machines are:

1. BuildNode: `sdp1.rtp.raleigh.ibm.com`
2. BuildNode2: `sdpbuild2.rtp.raleigh.ibm.com`
3. ODFTestEnv: `sdpdemo.rtp.raleigh.ibm.com`
4. BuildNodeWin1: `sdpwin1.rtp.raleigh.ibm.com`

Access user: ibmadmin / adm4sdp

These VMs can be managed through [vLaunch](https://vlaunch.rtp.raleigh.ibm.com/).


### Scripts / settings required on the build slave

#### Windows
On the windows slaves, install Git from IBM iRAM, e.g., [here](https://w3-03.ibm.com/tools/cm/iram/oslc/assets/503004E8-5971-230E-3D16-6F3FBDBE2E2C/2.5.1)
and make sure that the *bin* directory of the installation (typically something like `C:\Program Files (x86)\Git\bin`) is in the path.
This takes care that `sh.exe` is in the path and picked up by the Jenkins jobs.

#### `smashlittletonfirewall.sh`

Used to smash the Littleton firewall. Put this somewhere in the path, e.g., `~/bin`. The reason why this exists
at all is so that the intranet credentials don't appear in the build log. The file consists of this one line:

	curl -i -L  --user $INTRANETCREDENTIALS --insecure -X GET http://ips-rtc.swg.usma.ibm.com/jazz/web/projects
