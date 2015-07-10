package io.keen.client.java;

/**
 * QueryType specifies which query to run.
 *
 * Created by claireyoung on 6/16/15.
 */
public enum QueryType {
    COUNT_RESOURCE,
    COUNT_UNIQUE,
    MINIMUM_RESOURCE,
    MAXIMUM_RESOURCE,
    AVERAGE_RESOURCE,
    MEDIAN_RESOURCE,
    PERCENTILE_RESOURCE,
    SUM_RESOURCE,
    SELECT_UNIQUE_RESOURCE;

    /**
     * Gets the query type to send in the query HTTP request.
     *
     * @param type the QueryType
     * @return  the query string to specify in the HTTP request.
     *
     */
    public static final String getQueryType (QueryType type) {
        QueryType myType = QueryType.COUNT_RESOURCE;

        String queryString = "";
        switch (type) {
            case COUNT_RESOURCE:
                queryString = KeenQueryConstants.COUNT_RESOURCE;
                break;
            case COUNT_UNIQUE:
                queryString = KeenQueryConstants.COUNT_UNIQUE;
                break;
            case MINIMUM_RESOURCE:
                queryString = KeenQueryConstants.MINIMUM_RESOURCE;
                break;
            case MAXIMUM_RESOURCE:
                queryString = KeenQueryConstants.MAXIMUM_RESOURCE;
                break;
            case AVERAGE_RESOURCE:
                queryString = KeenQueryConstants.AVERAGE_RESOURCE;
                break;
            case MEDIAN_RESOURCE:
                queryString = KeenQueryConstants.MEDIAN_RESOURCE;
                break;
            case PERCENTILE_RESOURCE:
                queryString = KeenQueryConstants.PERCENTILE_RESOURCE;
                break;
            case SUM_RESOURCE:
                queryString = KeenQueryConstants.SUM_RESOURCE;
                break;
            case SELECT_UNIQUE_RESOURCE:
                queryString = KeenQueryConstants.SELECT_UNIQUE_RESOURCE;
                break;
            default:
                // TODO: throw exception or allow to be null?
                return queryString = null;
//                throw new KeenQueryClientException("Invalid query type input.");
        }
        return queryString;
    }
}
