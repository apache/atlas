package org.apache.metadata;


public class MetadataException extends Exception {

    public MetadataException() {
    }

    public MetadataException(String message) {
        super(message);
    }

    public MetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetadataException(Throwable cause) {
        super(cause);
    }

    public MetadataException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
