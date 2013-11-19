package io.keen.client.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * KeenHttpRequestRunnable implements java.util.concurrent.Runnable to send HTTP requests to Keen IO
 * in a background thread. Relies on a caller to invoke this correctly.
 * @author dkador
 * @since 1.0.0
 */
class KeenHttpRequestRunnable implements Runnable {
    private static final int READ_TIMEOUT = 60000;
    private static final int CONNECT_TIMEOUT = 60000;
    
    private final KeenClient keenClient;
    private final String eventCollection;
    private final Map<String, Object> event;
    private final AddEventCallback callback;

    KeenHttpRequestRunnable(KeenClient keenClient, String eventCollection, Map<String, Object> event, AddEventCallback callback) {
        this.keenClient = keenClient;
        this.eventCollection = eventCollection;
        this.event = event;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            HttpURLConnection connection = sendEvent(this.eventCollection, this.event);
            InputStream inputStream = connection.getInputStream();
            try {
                handleResult(inputStream, connection.getResponseCode(), callback);
            } finally {
                inputStream.close();
            }
        } catch (IOException e) {
            KeenLogging.log("There was an error while sending events to the Keen API.");
            String stackTrace = KeenUtils.getStackTraceFromThrowable(e);
            KeenLogging.log(stackTrace);
            if (callback != null) {
                callback.onError(stackTrace);
            }
        }
    }

    HttpURLConnection sendEvent(String eventCollection, Map<String, Object> event) throws IOException {
        // just using basic JDK HTTP library
        String urlString = String.format("%s/%s/projects/%s/events/%s", keenClient.getBaseUrl(),
                KeenConstants.API_VERSION, keenClient.getProjectId(), eventCollection);
        URL url = new URL(urlString);

        // set up the POST
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Authorization", keenClient.getWriteKey());
        connection.setReadTimeout(READ_TIMEOUT);
        connection.setConnectTimeout(CONNECT_TIMEOUT);
        // we're writing
        connection.setDoOutput(true);
        OutputStream out = connection.getOutputStream();
        // write JSON to the output stream
        KeenClient.MAPPER.writeValue(out, event);
        out.close();
        return connection;
    }

    static void handleResult(InputStream input, int responseCode, AddEventCallback callback) {
        if (responseCode == 201) {
            // event add worked
            if (callback != null) {
                // let the caller know if they've registered a callback
                callback.onSuccess();
            }
        } else {
            // if the response was bad, make a note of it
            KeenLogging.log(String.format("Response code was NOT 201. It was: %d", responseCode));
            String responseBody = KeenUtils.convertStreamToString(input);
            KeenLogging.log(String.format("Response body was: %s", responseBody));
            if (callback != null) {
                // let the caller know if they've registered a callback
                callback.onError(responseBody);
            }
        }
    }
}
