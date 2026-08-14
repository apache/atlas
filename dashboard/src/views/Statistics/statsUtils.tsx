/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Badge } from "@mui/material";
import { numberFormatWithComma } from "@utils/Helper";
import { formatedDate, millisecondsToTime } from "@utils/Utils";

export const getStatsValue = (options: { value: any; type: any }) => {
	const { value, type } = options;
	if (type == "time") {
		return millisecondsToTime(value);
	}
	if (type == "day") {
		return formatedDate({ date: value });
	}
	if (type == "number") {
		return numberFormatWithComma(value);
	}
	if (type == "millisecond") {
		return numberFormatWithComma(value) + " millisecond/s";
	}
	if (type == "status-html") {
		return <Badge color="success" variant="dot"></Badge>;
	}
	return value;
};
