package io.keen.client.java;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Created by claireyoung on 5/18/15.
 */
public class KeenQueryParams {
//    public String queryName;   // required

//    public String apiKey;      // if we use KeenClient
    public String eventCollection;     // required

    // mostly required
    public String targetProperty;

    // optional
    public List<Map<String, Object>> filters;
    public String timeframe;
    public String interval;
    public String timezone;
    public String groupBy;
    public String maxAge;

    public String percentile;
    public String email;
    public String latest;

    public KeenQueryParams() {
//        queryName = "";

//        apiKey = "";
        eventCollection = "";
        filters = null;
        timeframe = "";
        interval = "";
        timezone = "";
        groupBy = "";
        maxAge = "";
        targetProperty = "";
        percentile = "";
        email = "";
        latest = "";
    }

    public void setEventCollectionAndTargetProperty(String eventCollection, String targetProperty) {
        this.eventCollection = eventCollection;
        this.targetProperty = targetProperty;
    }

    public Map<String, Object> ConstructQueryArgs() {

        Map<String, Object> queryArgs = new HashMap<String, Object>();

        StringBuffer queryString = new StringBuffer();
        if (false == eventCollection.isEmpty()) {
            queryArgs.put(KeenQueryConstants.EVENT_COLLECTION, eventCollection);
        }

        if (false == timeframe.isEmpty()) {
            queryArgs.put(KeenQueryConstants.TIMEFRAME, timeframe);
        }

        if (false == interval.isEmpty()) {
            queryArgs.put(KeenQueryConstants.INTERVAL, interval);
        }

        if (false == timezone.isEmpty()) {
            queryArgs.put(KeenQueryConstants.TIMEZONE, timezone);
        }

        if (false == groupBy.isEmpty()) {
            queryArgs.put(KeenQueryConstants.GROUP_BY, groupBy);
        }

        if (false == maxAge.isEmpty()) {
            queryArgs.put(KeenQueryConstants.MAX_AGE, maxAge);
        }

        if (false == targetProperty.isEmpty()) {
            queryArgs.put(KeenQueryConstants.TARGET_PROPERTY, targetProperty);
        }

        if (false == percentile.isEmpty()) {
            queryArgs.put(KeenQueryConstants.PERCENTILE, targetProperty);
        }

        if (false == email.isEmpty()) {
            queryArgs.put(KeenQueryConstants.EMAIL, email);
        }

        if (false == latest.isEmpty()) {
            queryArgs.put(KeenQueryConstants.LATEST, latest);
        }

        if (filters != null && filters.isEmpty() == false) {
            queryArgs.put(KeenQueryConstants.FILTERS, filters);
        }

        return queryArgs;
    }

    // TODO: should we validate that the filter params are valid???
    public void addFilter(String propertyName, String operator, String propertyValue) {
        Map<String, Object> filter = new HashMap<String, Object>();
        filter.put(KeenQueryConstants.PROPERTY_NAME, propertyName);
        filter.put(KeenQueryConstants.OPERATOR, operator);
        filter.put(KeenQueryConstants.PROPERTY_VALUE, propertyValue);

        if (this.filters == null) {
            this.filters = new ArrayList<Map<String, Object>>();
        }

        if (filters == null) {
            filters = new ArrayList<Map<String, Object>>();
        }

        filters.add(filter);
    }

    // TODO: need to format for HTML
    public String ConstructQueryArgsForHTML() {

        StringBuffer queryString = new StringBuffer();
        if (false == eventCollection.isEmpty()) {
            queryString.append("?");
            queryString.append(KeenQueryConstants.EVENT_COLLECTION);
            queryString.append("=");
            queryString.append(eventCollection);
        }

        if (false == timeframe.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.TIMEFRAME);
            queryString.append("=");
            queryString.append(timeframe);
        }

        if (false == interval.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.INTERVAL);
            queryString.append("=");
            queryString.append(interval);
        }

        if (false == timezone.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.TIMEZONE);
            queryString.append("=");
            queryString.append(timezone);
        }

        if (false == groupBy.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.GROUP_BY);
            queryString.append("=");
            queryString.append(groupBy);
        }

        if (false == maxAge.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.MAX_AGE);
            queryString.append("=");
            queryString.append(maxAge);
        }

        if (false == targetProperty.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.TARGET_PROPERTY);
            queryString.append("=");
            queryString.append(targetProperty);
        }

        if (false == percentile.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.PERCENTILE);
            queryString.append("=");
            queryString.append(percentile);
        }

        if (false == email.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.EMAIL);
            queryString.append("=");
            queryString.append(email);
        }

        if (false == latest.isEmpty()) {
            queryString.append("&");
            queryString.append(KeenQueryConstants.LATEST);
            queryString.append("=");
            queryString.append(latest);
        }

        return queryString.toString();
    }

    public boolean AreParamsValid(String queryName) {
        if (queryName.isEmpty()) {
            return false;
        }

        if (queryName.contentEquals(KeenQueryConstants.COUNT_RESOURCE) || queryName.contentEquals(KeenQueryConstants.EXTRACTION_RESOURCE)) {
            if (eventCollection.isEmpty()) {
                return false;
            }
        }

        if (queryName.contentEquals(KeenQueryConstants.COUNT_UNIQUE)
                || queryName.contentEquals(KeenQueryConstants.MINIMUM_RESOURCE) || queryName.contentEquals(KeenQueryConstants.MAXIMUM_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.AVERAGE_RESOURCE) || queryName.contentEquals(KeenQueryConstants.MEDIAN_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.PERCENTILE_RESOURCE) || queryName.contentEquals(KeenQueryConstants.SUM_RESOURCE)
                || queryName.contentEquals(KeenQueryConstants.SELECT_UNIQUE_RESOURCE)) {
            if (eventCollection.isEmpty()) {
                return false;
            }
            if (targetProperty.isEmpty()) {
                return false;
            }
        }

        if (queryName.contentEquals(KeenQueryConstants.PERCENTILE_RESOURCE)) {
            if (percentile.isEmpty()) {
                return false;
            }
        }

        // TODO: funnel, multi-analysis, wardrobe

        return true;
    }

}
