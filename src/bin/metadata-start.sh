#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source ${DIR}/metadata-config.sh

nohup ${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${METADATACPPATH} org.apache.hadoop.metadata.Main -app ${BASEDIR}/server/webapp/metadata $* > "${METADATA_LOG_DIR}/metadata-server.$TIME.out" 2>&1 &
echo $! > $METADATA_PID_FILE
popd > /dev/null

echo Metadata Server started!!!
