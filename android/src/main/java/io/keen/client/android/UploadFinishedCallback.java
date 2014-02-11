package io.keen.client.android;

/**
 * An interface to simulate functional programming so that the {@link io.keen.client.android.KeenClient} can
 * notify you when an asynchronous upload has finished.
 *
 * @author dkador
 * @since 1.0.0
 */
public interface UploadFinishedCallback {
    void callback();
}
