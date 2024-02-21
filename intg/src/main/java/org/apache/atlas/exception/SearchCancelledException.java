package org.apache.atlas.exception;

public class SearchCancelledException extends Exception {
    public SearchCancelledException(String message) {
        super(message);
    }

    public SearchCancelledException(String message, Throwable cause) {
        super(message, cause);
    }

    public SearchCancelledException(Throwable cause) {
        super(cause);
    }
}
