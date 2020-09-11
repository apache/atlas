#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "${BRANCH}" == "" ]
then
  BRANCH=master
fi

if [ "${PROFILE}" != "" ]
then
  ARG_PROFILES="-P${PROFILE}"
fi

if [ "${SKIPTESTS}" == "" ]
then
  ARG_SKIPTESTS="-DskipTests"
else
  ARG_SKIPTESTS="-DskipTests=${SKIPTESTS}"
fi

export MAVEN_OPTS="-Xms2g -Xmx2g"
export M2=/home/atlas/.m2


if [ -f /home/atlas/src/pom.xml ]
then
  echo "Building from /home/atlas/src"

  cd /home/atlas/src
else
  echo "Building from /home/atlas/git/atlas"

  cd /home/atlas/git/atlas

  git checkout ${BRANCH}
  git pull

  for patch in `ls -1 /home/atlas/patches | sort`
  do
    echo "applying patch /home/atlas/patches/${patch}"
    git apply /home/atlas/patches/${patch}
  done
fi

mvn ${ARG_PROFILES} ${ARG_SKIPTESTS} -DskipDocs clean package

mv -f distro/target/apache-atlas-${ATLAS_VERSION}-bin.tar.gz /home/atlas/dist/
