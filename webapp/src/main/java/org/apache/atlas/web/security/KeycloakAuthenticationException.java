package org.apache.atlas.web.security;

import org.springframework.security.core.AuthenticationException;

public class KeycloakAuthenticationException extends AuthenticationException {
    public KeycloakAuthenticationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public KeycloakAuthenticationException(String msg) {
        super(msg);
    }
}
