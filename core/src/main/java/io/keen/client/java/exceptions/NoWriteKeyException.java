package io.keen.client.java.exceptions;

/**
 * NoWriteKeyException
 *
 * @author dkador
 * @since 1.0.1
 */
public class NoWriteKeyException extends KeenException {
    public NoWriteKeyException(String detailMessage) {
        super(detailMessage);
    }
}