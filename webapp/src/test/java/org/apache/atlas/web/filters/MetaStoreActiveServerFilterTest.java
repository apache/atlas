package org.apache.atlas.web.filters;

import org.junit.Test;


import static org.apache.atlas.web.filters.ActiveServerFilter.sanitizeRedirectLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MetaStoreActiveServerFilterTest {

    @Test
    public void testSanitizeRedirectLocation() {
        Object[][] testCases = {
                {"https://dom-sub-uat.atlan.com/api/meta/entity/guid/fd7a69c9-738b-4b35-a0db-1da00cbd86cd", "https%3A%2F%2Fdom-sub-uat.atlan.com%2Fapi%2Fmeta%2Fentity%2Fguid%2Ffd7a69c9-738b-4b35-a0db-1da00cbd86cd"},
                {"https://datamesh.atlan.com/api/meta/entity/bulk?replaceBusinessAttributes=true&replaceClassifications=true", "https%3A%2F%2Fdatamesh.atlan.com%2Fapi%2Fmeta%2Fentity%2Fbulk%3FreplaceBusinessAttributes%3Dtrue%26replaceClassifications%3Dtrue"},
                {"http://example.com/page?param=value&another=one", "http%3A%2F%2Fexample.com%2Fpage%3Fparam%3Dvalue%26another%3Done"},
                {"http://example.com/page?param=value%Set-Cookie: test=evil", "http%3A%2F%2Fexample.com%2Fpage%3Fparam%3Dvalue%25Set-Cookie%3A+test%3Devil"},
                {"http://example.com/search?query=value\n<script>alert('xss')</script>", "http%3A%2F%2Fexample.com%2Fsearch%3Fquery%3Dvalue%3Cscript%3Ealert%28%27xss%27%29%3C%2Fscript%3E"},
                {"http://example.com/update?action=edit%HTTP/1.1 200 OKContent-Type: text/html", "http%3A%2F%2Fexample.com%2Fupdate%3Faction%3Dedit%25HTTP%2F1.1+200+OKContent-Type%3A+text%2Fhtml"},
                {"http://example.com/login?redirect=success%Set-Cookie: sessionId=12345", "http%3A%2F%2Fexample.com%2Flogin%3Fredirect%3Dsuccess%25Set-Cookie%3A+sessionId%3D12345"},
                {"http://example.com/page\r", "http%3A%2F%2Fexample.com%2Fpage"},
                {"http://example.com/page?next=url%0D%0AContent-Length: %300", "http%3A%2F%2Fexample.com%2Fpage%3Fnext%3Durl%0D%0AContent-Length%3A+%300"},
                {null, null}  // Testing for null input
        };

        for (Object[] testCase : testCases) {
            String input = (String) testCase[0];
            String expected = (String) testCase[1];

            if (input == null) {
                assertNull("Output should be null for null input.", sanitizeRedirectLocation(input));
            } else {
                assertEquals("URLs should be correctly sanitized.", expected, sanitizeRedirectLocation(input));
            }
        }
    }

}
