package org.apache.atlas.web.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.Authentication;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AtlasAuthenticationSuccessHandlerTest {
    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;
    @Mock
    private HttpSession session;
    @Mock
    private Authentication authentication;

    @Test
    public void onAuthenticationSuccessSetsOnlySessionMarker() throws Exception {
        AtlasAuthenticationSuccessHandler handler = new AtlasAuthenticationSuccessHandler();
        StringWriter body = new StringWriter();

        when(request.getSession()).thenReturn(session);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        handler.onAuthenticationSuccess(request, response, authentication);

        verify(session).setAttribute(AtlasAuthenticationSuccessHandler.LOCALLOGIN, "true");
        verify(session).setMaxInactiveInterval(anyInt());
        verify(request, never()).getServletContext();
        assertTrue(body.toString().contains("Success"));
    }
}
