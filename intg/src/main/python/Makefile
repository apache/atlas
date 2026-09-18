##########################################################################
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
##########################################################################

PYTHONPATH ?= .
TEST_FILE  ?= ../../test/python/test_atlas_client.py

.PHONY: test_unit lint mypy test build check

test_unit:
	PYTHONPATH=$(PYTHONPATH) python3 -B $(TEST_FILE)

lint:
	python3 -m flake8 apache_atlas

mypy:
	python3 -m mypy --ignore-missing-imports --follow-imports=skip --strict-optional --warn-no-return apache_atlas

test: test_unit lint mypy

build:
	python3 -m build

check: build
	twine check dist/*
