package org.apache.atlas.web.filters;

import org.junit.Test;


import static org.apache.atlas.web.filters.ActiveServerFilter.sanitizeRedirectLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MetaStoreActiveServerFilterTest {

    @Test
    public void testSanitizeRedirectLocation_WithValidUrl() {
        String testUrl = "http://example.com/page?param=value";
        String expected = "http%3A%2F%2Fexample.com%2Fpage%3Fparam%3Dvalue";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("The URLs do not match.",expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_WithNull() {
        assertNull("Output should be null for null input.",sanitizeRedirectLocation(null));
    }




    @Test
    public void testSanitizeRedirectLocation_WithSpecialCharacters() {
        String testUrl = "http://example.com/page?param=value&another=one";
        String expected = "http%3A%2F%2Fexample.com%2Fpage%3Fparam%3Dvalue%26another%3Done";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("Special characters should be URL encoded.", expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_CorruptingCharactersForHttpSplitting() {
        String testUrl = "http://example.com/page?param=value%Set-Cookie: test=evil";
        String expected = "http%3A%2F%2Fexample.com%2Fpage%3Fparam%3Dvalue%25Set-Cookie%3A+test%3Devil";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("HTTP response splitting characters and other specials should be properly encoded.", expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_MultiLineQueryParameter() {
        String testUrl = "http://example.com/search?query=value\n<script>alert('xss')</script>";
        String expected = "http%3A%2F%2Fexample.com%2Fsearch%3Fquery%3Dvalue%3Cscript%3Ealert%28%27xss%27%29%3C%2Fscript%3E";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("Multi-line and script injection attempts should be encoded.", expected, actual);
    }


    @Test
    public void testSanitizeRedirectLocation_CRLFInjectionToSplitResponse() {
        String testUrl = "http://example.com/update?action=edit%HTTP/1.1 200 OKContent-Type: text/html";
        String expected = "http%3A%2F%2Fexample.com%2Fupdate%3Faction%3Dedit%25HTTP%2F1.1+200+OKContent-Type%3A+text%2Fhtml";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("CRLF characters used to split HTTP responses should be properly encoded.", expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_HeaderInjectionViaNewline() {
        String testUrl = "http://example.com/login?redirect=success%Set-Cookie: sessionId=12345";
        String expected = "http%3A%2F%2Fexample.com%2Flogin%3Fredirect%3Dsuccess%25Set-Cookie%3A+sessionId%3D12345";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("Characters potentially harmful for HTTP response splitting should be encoded.", expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_CRLFRemoved() {
        String testUrl = "http://example.com/page\r";
        String expected = "http%3A%2F%2Fexample.com%2Fpage";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("Carriage return characters should be removed.", expected, actual);
    }

    @Test
    public void testSanitizeRedirectLocation_EncodedLineBreaks() {
        String testUrl = "http://example.com/page?next=url%0D%0AContent-Length: %300";
        String expected = "http%3A%2F%2Fexample.com%2Fpage%3Fnext%3Durl%0D%0AContent-Length%3A+%300";
        String actual = sanitizeRedirectLocation(testUrl);
        assertEquals("Encoded line breaks and attempts to continue headers should be removed.", expected, actual);
    }

}
