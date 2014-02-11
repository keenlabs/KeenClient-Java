package io.keen.client.java;

/**
 * An interface to simulate functional programming so that the {@link io.keen.client.java.KeenClient} can
 * notify you when an asynchronous HTTP request succeeds or fails.
 *
 * @author dkador
 * @since 1.0.0
 */
public interface AddEventCallback {
    /**
     * Invoked when adding the event succeeds.
     */
    public void onSuccess();

    /**
     * Invoked when adding the event fails.
     *
     * @param responseBody The HTTP body of the response as a string.
     */
    public void onError(String responseBody);
}
