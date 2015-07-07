Keen Query Clients
===================

[![Build Status](https://travis-ci.org/keenlabs/KeenClient-Java.png?branch=master)](https://travis-ci.org/keenlabs/KeenClient-Java)

The query capabilities within the Java Keen client enable you to send POST queries and receive the results of the queries in a JSON object. For query types, refer to API technical reference: https://keen.io/docs/api/reference/.

## Usage
### Building a Keen Query Client
You can build a KeenQueryClient by just providing a KeenProject.
```java
KeenProject queryProject = new KeenProject("<project id>", "<write key>", "<read key>");
KeenQueryClient queryClient = new TestKeenQueryClientBuilder(queryProject).build();
```
Optionally, users can also specify a HTTP Handler, base URL, or JSON Handler:
```java
KeenQueryClient queryClient = new TestKeenQueryClientBuilder(queryProject)
		.withHttpHandler(httpHandler)
		.withJsonHandler(jsonHandler)
		.withBaseUrl(baseURL)
		.build();
```
### Using the KeenQueryClient to send Queries
The most simple way that users can use the KeenQueryClient to send queries is as follows. These methods take only the required query parameters as input, and the user receives a very specific Integer or Double response type. Please note that we strongly encourage users to pass in the Timeframe parameter, but it can be null.
```java
Long count = queryClient.count("<event_collection>", new RelativeTimeframe("this_year"));
Long countUnique = queryClient.countUnique("<event_collection>", "<target_property>", new AbsoluteTimeframe("2012-08-13T19:00:00.000Z","2015-06-07T19:00:00.000Z"));
Double minimum = queryClient.minimum("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
Double maximum = queryClient.maximum("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
Double average = queryClient.average("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
Double median = queryClient.median("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
Double percentile = queryClient.percentile("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
Double sum = queryClient.sum("<event_collection>", "<target_property>", new RelativeTimeframe("this_year"));
```
The exceptions are Select Unique, Extraction, Funnel, and Multi-Analysis queries which are more a little more complicated and are mentioned below (TODO: Mention below!!!).

### Advanced
Alternatively, users can use optional parameters to send queries. However, because the result may be different depending on the optional parameters, the return value is a QueryResult. The user is expected to verify the expected return type for the query, given the parameters entered.
```java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .build();
QueryResult result = queryClient.execute(query, new RelativeTimeframe("this_month"));
if (result.isLong()) {
	long countValue = result.longValue();
	// do something with countValue
}
```

Some special cases are when filters "Group By" and "Inteval" are specified, as well as the Select Unique query.

For Select Unique queries, the user gets a list of unique values, given the target property. Therefore, the QueryResult will be a list of unique property values. The QueryResult type only supports Integer, Double, String, and List values; therefore, if the property value is not one of the aforementioned types, then you may not be able to access that value.

``` java
Query query = new QueryBuilder(QueryType.SELECT_UNIQUE_RESOURCE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .build();
QueryResult result = queryClient.execute(query, new RelativeTimeframe("this_month"));
if (result.isListResult()) {
	ArrayList<QueryResult> listResults = result.getListResults();
	for (QueryResult item : listResults) {
		if (item.isLong()) {
			// do something with long value
		}
	}
}
```

Filtering any query via Group-By will cause the query response to be a GroupByResult object. This object stores Map<Group, QueryResult> objects, where the Group contains the unique property/value pairs.
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new RelativeTimeframe("this_month"));
if (result.isGroupResult()) {
	for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
	    Map<String, Object> groupProperies = groupResult.getKey().getProperties();
	    long groupCount = groupResult.getValue().longValue();
	    // ... do something with the group properties and the count result
	}
}
```
Filtering any query via Interval will cause the query response to be an IntervalResult object. An IntervalResult is a type of QueryResult that consist of Map<AbsoluteTimeframe,QueryResult> objects.
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .build();
QueryResult result = queryClient.execute(query, new RelativeTimeframe("this_year"));
if (result.isIntervalResult()) {
        for (Map.Entry<AbsoluteTimeframe, QueryResult> intervalResult : result.getIntervalResults().entrySet()) {
            AbsoluteTimeframe timeframe = intervalResult.getKey();
            long intervalCount = intervalResult.getValue().longValue();
            // ... do something with the absolute timeframe and count result.
        }
}      
```
Filtering via both Group By and Interval will cause the query response to be an IntervalResult object that contains GroupByResult objects follows:

``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new RelativeTimeframe("this_year"));

if (result.isIntervalResult()) {
    for (Map.Entry<AbsoluteTimeframe, QueryResult> intervalResult : result.getIntervalResults().entrySet()) {
        AbsoluteTimeframe timeframe = intervalResult.getKey();

        for (Map.Entry<Group, QueryResult> groupResult : intervalResult.getValue().getGroupResults().entrySet()) {
            Map<String, Object> groupProperies = groupResult.getKey().getProperties();
            long groupCount = groupResult.getValue().longValue();
            // ... do something with the group properties and the count result
        }
    }
}
```

### Utility Methods

There are also some utility methods to add filters and absolute timeframes to a Query:
```java

// notice this will add two filter parameters, with 1 < click-count < 5
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
	            .withEventCollection("<event_collection>")
	            .withFilter("click-count", "lt", 5)
	            .withFilter("click-count", "gt", 1)
	            .build();

Object result = queryClient.count(query, new RelativeTimeframe("<start>", "<end>"));
Integer queryResult = null;
if (result instance Integer) {
	queryResult = (Integer)result;
}
```


## Changelog

##### 1.0
Introduce KeenQueryClient.