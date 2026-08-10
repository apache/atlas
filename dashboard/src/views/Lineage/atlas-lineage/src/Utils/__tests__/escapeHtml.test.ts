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

jest.mock("../LineageUtils", () => ({ default: {} }));
jest.mock("../DataUtils", () => ({ default: {} }));

import { escapeHtml } from "../index";

/**
 * Mirrors underscore _.escape used in classic UI:
 * - atlas-lineage (dashboardv2/external_lib)
 * - ProfileBarChart.js
 */
const underscoreEscape = (value: unknown): string => {
	if (value == null) {
		return "";
	}

	const escapeMap: Record<string, string> = {
		"&": "&amp;",
		"<": "&lt;",
		">": "&gt;",
		'"': "&quot;",
		"'": "&#x27;",
		"`": "&#x60;"
	};

	return String(value).replace(/[&<>"'`]/g, (match) => escapeMap[match]);
};

const expectSafeTooltipText = (encoded: string) => {
	expect(encoded).not.toMatch(/<[^&!]/);
	expect(encoded).not.toContain("<script");
	expect(encoded).not.toContain("<img");
};

describe("lineage tooltip escapeHtml (React)", () => {
	describe("HTML metacharacter encoding", () => {
		it("escapes angle brackets in malicious tooltip labels", () => {
			const payload = '<img src=x onerror="window.__xss=1">';
			const escaped = escapeHtml(payload);

			expect(escaped).not.toContain("<img");
			expect(escaped).toContain("&lt;img");
			expect(escaped).toContain("&quot;");
			expectSafeTooltipText(escaped);
		});

		it("escapes script tags", () => {
			const escaped = escapeHtml('<script>alert("x")</script>');
			expect(escaped).toBe("&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;");
			expectSafeTooltipText(escaped);
		});

		it("escapes ampersand", () => {
			expect(escapeHtml("a & b")).toBe("a &amp; b");
		});

		it("double-encodes already-encoded ampersand entities safely", () => {
			expect(escapeHtml("a &amp; b")).toBe("a &amp;amp; b");
			expectSafeTooltipText(escapeHtml("a &amp; b"));
		});

		it("escapes single quotes", () => {
			expect(escapeHtml("O'Brien")).toBe("O&#39;Brien");
		});

		it("escapes double quotes", () => {
			expect(escapeHtml('say "hello"')).toBe("say &quot;hello&quot;");
		});

		it("escapes all special characters in one payload", () => {
			const payload = `<a href='x'>&"test"`;
			const escaped = escapeHtml(payload);
			expect(escaped).toBe("&lt;a href=&#39;x&#39;&gt;&amp;&quot;test&quot;");
			expectSafeTooltipText(escaped);
		});
	});

	describe("nullish and empty inputs", () => {
		it("handles null and undefined values", () => {
			expect(escapeHtml(null)).toBe("");
			expect(escapeHtml(undefined)).toBe("");
		});

		it("handles empty string", () => {
			expect(escapeHtml("")).toBe("");
		});

		it("handles whitespace-only label used when displayText is missing", () => {
			expect(escapeHtml(" ")).toBe(" ");
		});
	});

	describe("numeric and coerced values", () => {
		it("coerces numeric values to string", () => {
			expect(escapeHtml(123)).toBe("123");
			expect(escapeHtml(0)).toBe("0");
		});

		it("coerces boolean values to string", () => {
			expect(escapeHtml(true)).toBe("true");
			expect(escapeHtml(false)).toBe("false");
		});
	});

	describe("plain text preservation", () => {
		it("preserves safe entity display names", () => {
			expect(escapeHtml("sales_db.customers")).toBe("sales_db.customers");
		});

		it("preserves qualified names with dots and underscores", () => {
			expect(escapeHtml("hive.db.table_v1")).toBe("hive.db.table_v1");
		});

		it("preserves unicode characters in entity names", () => {
			expect(escapeHtml("データ_テーブル")).toBe("データ_テーブル");
		});

		it("preserves newlines in multiline query text", () => {
			const queryText = "SELECT *\nFROM t\nWHERE id = 1";
			expect(escapeHtml(queryText)).toBe(queryText);
			expectSafeTooltipText(escapeHtml(queryText));
		});
	});

	describe("lineage tooltip field scenarios", () => {
		it("encodes toolTipLabel from entity displayText", () => {
			const label = 'db.table<script>alert(1)</script>';
			const escaped = escapeHtml(label);
			expect(escaped).toBe("db.table&lt;script&gt;alert(1)&lt;/script&gt;");
			expectSafeTooltipText(escaped);
		});

		it("encodes typeName values", () => {
			const escaped = escapeHtml("hive_table<script>");
			expect(escaped).toBe("hive_table&lt;script&gt;");
			expectSafeTooltipText(escaped);
		});

		it("encodes queryText with SQL quotes and comparison operators", () => {
			const queryText =
				"SELECT * FROM t WHERE x < 1 AND name = \"foo\" AND y > 0";
			const escaped = escapeHtml(queryText);

			expect(escaped).toContain("&lt;");
			expect(escaped).toContain("&gt;");
			expect(escaped).toContain("&quot;");
			expectSafeTooltipText(escaped);
		});

		it("encodes static toolTipTitle values", () => {
			expect(escapeHtml("Type")).toBe("Type");
			expect(escapeHtml('Type<script>')).toBe("Type&lt;script&gt;");
		});
	});

	describe("intentional differences from classic _.escape", () => {
		it("does not encode backtick in text content (safe inside span tags)", () => {
			expect(escapeHtml("db`table")).toBe("db`table");
			expectSafeTooltipText(escapeHtml("db`table"));
		});
	});
});

describe("classic UI tooltip encoding (underscore _.escape parity)", () => {
	const tooltipPayloads = [
		'<img src=x onerror="alert(1)">',
		"O'Brien",
		"a & b",
		"sales_db.customers",
		123,
		"",
		null,
		undefined
	];

	it.each(tooltipPayloads)(
		"underscoreEscape produces safe output for %p",
		(payload) => {
			const escaped = underscoreEscape(payload);

			if (payload == null) {
				expect(escaped).toBe("");
				return;
			}

			expectSafeTooltipText(escaped);

			const raw = String(payload);
			if (/[&<>"'`]/.test(raw)) {
				expect(escaped).not.toBe(raw);
			} else {
				expect(escaped).toBe(raw);
			}
		}
	);

	it("handles null and undefined like production _.escape", () => {
		expect(underscoreEscape(null)).toBe("");
		expect(underscoreEscape(undefined)).toBe("");
	});

	it("preserves numeric count values for profile bar chart tooltips", () => {
		expect(underscoreEscape(42)).toBe("42");
		expect(underscoreEscape(0)).toBe("0");
	});

	it("encodes lineage queryText with quotes and angle brackets", () => {
		const queryText = 'SELECT * FROM t WHERE x < 1 AND name = "foo"';
		const escaped = underscoreEscape(queryText);

		expect(escaped).toContain("&lt;");
		expect(escaped).toContain("&quot;");
		expectSafeTooltipText(escaped);
	});

	it("encodes lineage typeName values", () => {
		const escaped = underscoreEscape("hive_table<script>");
		expect(escaped).toBe("hive_table&lt;script&gt;");
		expectSafeTooltipText(escaped);
	});

	it("encodes backtick characters used in entity names", () => {
		expect(underscoreEscape("db`table")).toBe("db&#x60;table");
	});

	it("react escapeHtml and classic _.escape both produce safe tooltip output", () => {
		const payloads = [
			'<img src=x onerror="alert(1)">',
			'<script>alert(1)</script>',
			"O'Brien",
			"a & b",
			"hive_process",
			'SELECT * FROM t WHERE x < 1 AND name = "foo"',
			"hive_table<script>"
		];

		payloads.forEach((payload) => {
			expectSafeTooltipText(escapeHtml(payload));
			expectSafeTooltipText(underscoreEscape(payload));
		});
	});
});
