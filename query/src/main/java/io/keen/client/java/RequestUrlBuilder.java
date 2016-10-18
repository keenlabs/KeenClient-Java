/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class which handles formatting of request URLs.
 * @author baumatron
 */
public class RequestUrlBuilder {
    
    // The API version string
    private final String apiVersion;
    
    // The base URL, including the scheme and domain
    private final String baseUrl;
    
    public RequestUrlBuilder(String apiVersion, String baseUrl)
    {
        if (null == apiVersion || apiVersion.isEmpty())
        {
            throw new IllegalArgumentException("RequestUrlBuilder apiVersion is a required argument.");
        }
        
        if (null == baseUrl || baseUrl.isEmpty())
        {
            throw new IllegalArgumentException("RequestUrlBuilder baseUrl is a required argument.");
        }
        
        this.apiVersion = apiVersion;
        this.baseUrl = baseUrl;
    }
    
    /**
     * Get a format URL for an analysis request.
     * @param projectId The project id
     * @param analysisName The analysis name
     * @return The complete URL.
     * @throws KeenQueryClientException
     */
    public URL getAnalysisUrl(String projectId, String analysisName) throws KeenQueryClientException
    {
        try {
            return new URL(String.format(
                    Locale.US,
                    "%s/%s/projects/%s/queries/%s",
                    this.baseUrl,
                    this.apiVersion,
                    projectId,
                    analysisName
            ));
        } catch (MalformedURLException ex) {
            Logger.getLogger(RequestUrlBuilder.class.getName()).log(Level.SEVERE, "Failed to format single analysis URL.", ex);
            throw new KeenQueryClientException("Failed to format analysis URL.");
        }
    }
}
