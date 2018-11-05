package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class which handles formatting of request URLs.
 *
 * @author baumatron
 */
class RequestUrlBuilder {
    // The API version string
    private final String apiVersion;

    // The base URL, including the scheme and domain
    private final String baseUrl;

    RequestUrlBuilder(String apiVersion, String baseUrl) {
        if (null == apiVersion || apiVersion.trim().isEmpty()) {
            throw new IllegalArgumentException("'apiVersion' is a required argument.");
        }

        if (null == baseUrl || baseUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("'baseUrl' is a required argument.");
        }

        this.apiVersion = apiVersion;
        this.baseUrl = baseUrl;
    }

    /**
     * Get a formatted URL for an analysis request.
     *
     * @param projectId    The project id
     * @param analysisPath The analysis url sub-path
     * @return The complete URL.
     * @throws KeenQueryClientException
     */
    URL getAnalysisUrl(String projectId, String analysisPath) throws KeenQueryClientException {
        try {
            return new URL(String.format(Locale.US,
                    "%s/%s/projects/%s/queries/%s",
                    this.baseUrl,
                    this.apiVersion,
                    projectId,
                    analysisPath
            ));
        } catch (MalformedURLException ex) {
            Logger.getLogger(RequestUrlBuilder.class.getName())
                    .log(Level.SEVERE, "Failed to format query URL.", ex);

            throw new KeenQueryClientException("Failed to format query URL.", ex);
        }
    }

    URL getDatasetsUrl(String projectId, String datasetName, String suffix, Map<String, Object> queryParams) throws KeenQueryClientException {
        try {
            StringBuilder url = new StringBuilder(String.format(Locale.US,
                    "%s/%s/projects/%s/%s/%s",
                    this.baseUrl,
                    this.apiVersion,
                    projectId,
                    KeenQueryConstants.DATASETS,
                    datasetName
            ));
            if (suffix != null) {
                url.append(suffix);
            }

            if (queryParams != null && !queryParams.isEmpty()) {
                url.append("?");
                for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
                    url.append(String.format("%s=%s&", entry.getKey(), entry.getValue()));
                }
            }

            return new URL(url.toString().replaceFirst("&$", ""));
        } catch (MalformedURLException ex) {
            Logger.getLogger(RequestUrlBuilder.class.getName())
                    .log(Level.SEVERE, "Failed to format query URL.", ex);

            throw new KeenQueryClientException("Failed to format query URL.", ex);
        }
    }
}
