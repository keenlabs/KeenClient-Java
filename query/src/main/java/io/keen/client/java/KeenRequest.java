/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.keen.client.java;

import io.keen.client.java.exceptions.KeenQueryClientException;
import java.net.URL;
import java.util.Map;

/**
 *
 * @author baumatron
 */
public interface KeenRequest {
    
    public URL getRequestURL(RequestUrlBuilder urlBuilder, String projectId) throws KeenQueryClientException;
    
    public Map<String, Object> constructRequestArgs();
    
    public boolean groupedResponseExpected();
    
    public boolean intervalResponseExpected();
}
