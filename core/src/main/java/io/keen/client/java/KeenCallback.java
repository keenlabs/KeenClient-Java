package io.keen.client.java;

/**
 * An interface to simulate functional programming so that the {@link io.keen.client.java.KeenClient} can
 * notify you when an asynchronous HTTP request succeeds or fails.
 * TODO: Update this comment
 *
 * @author Kevin Litwack
 * @since 2.0.0
 */
public interface KeenCallback {
    /**
     * Invoked when the requested operation succeeds.
     */
    public void onSuccess();

    /**
     * Invoked when the requested operation fails.
     *
     * @param e An exception indicating the cause of the failure.
     */
    public void onFailure(Exception e);
}
