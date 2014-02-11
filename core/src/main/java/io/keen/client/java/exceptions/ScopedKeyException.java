package io.keen.client.java.exceptions;

/**
 * ScopedKeyException
 *
 * @author dkador
 * @since 1.0.3
 */
public class ScopedKeyException extends KeenException {
    public ScopedKeyException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
