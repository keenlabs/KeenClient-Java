package io.keen.client.java.exceptions;

/**
 * KeenException
 *
 * @author dkador
 * @since 1.0.0
 */
public abstract class KeenException extends Exception {
    KeenException(String detailMessage) {
        super(detailMessage);
    }

    public KeenException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
