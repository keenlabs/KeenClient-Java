package io.keen.client.java.exceptions;

/**
 * Exceptions thrown by KeenQueryClient. This includes errors reported by the server.
 *
 * Created by claireyoung on 5/27/15.
 */
public class KeenQueryClientException extends KeenException {
    private static final long serialVersionUID = -8714276738565293346L;

    public KeenQueryClientException() {
        super();
    }

    public KeenQueryClientException(Throwable cause) {
        super(cause);
    }

    public KeenQueryClientException(String message) {
        super(message);
    }

    public KeenQueryClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
