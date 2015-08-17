Keen Query Clients
===================

The query capabilities within the Java Keen client enable you to send POST queries and receive the results of the queries in a JSON object. For query types, refer to [API technical reference](https://keen.io/docs/api/reference/).

## Usage
### Building a Keen Query Client
You can build a KeenQueryClient by just providing a KeenProject. Note that for query purposes, the write key is not required. It is therefore OK and normal to provide ```null``` argument for the write key, unless that same KeenProject will be used for publishing events as well.
```java
KeenProject queryProject = new KeenProject("<project id>", "<write key>", "<read key>");
KeenQueryClient queryClient = new KeenQueryClient.Builder(queryProject).build();
```
Optionally, users can also specify a HTTP Handler, base URL, or JSON Handler:
```java
KeenQueryClient queryClient = new KeenQueryClient.Builder(queryProject)
		.withHttpHandler(httpHandler)
		.withJsonHandler(jsonHandler)
		.withBaseUrl(baseURL)
		.build();
```
### Using the KeenQueryClient to send Queries
The most simple way that users can use the KeenQueryClient to send queries is as follows. These methods take only the required query parameters as input, and the user receives a very specific ```long``` or ```double``` response type. Please note that we strongly encourage users to pass in the Timeframe parameter, but it can be null.
```java
long count = queryClient.count("<event_collection>", new RelativeTimeframe("this_week"));
long countUnique = queryClient.countUnique("<event_collection>", "<target_property>", new AbsoluteTimeframe("2015-05-15T19:00:00.000Z","2015-06-07T19:00:00.000Z"));
double minimum = queryClient.minimum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double maximum = queryClient.maximum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double average = queryClient.average("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double median = queryClient.median("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double percentile = queryClient.percentile("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
double sum = queryClient.sum("<event_collection>", "<target_property>", new RelativeTimeframe("this_week"));
```
The exceptions are Select Unique, Extraction, Funnel, and Multi-Analysis queries. These queries are a little more complicated, and only the Select Unique query is included in the initial release of the Keen Query Client.

### Advanced
Alternatively, users can use optional parameters to send queries. The return type is a QueryResult object. The user is expected to verify the expected QueryResult subclass, given the parameters entered.
```java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isLong()) {
	long countValue = result.longValue();
	// do something with countValue
}
```

Some special cases are when "Group By" and "Interval" are specified, as well as the Select Unique query.

Select Unique queries return a list of unique values, given the target property. Therefore, the QueryResult will be a list of unique property values. The QueryResult type only supports Integer, Double, String, and List values; therefore, if the property value is not one of the aforementioned types, then you may not be able to access that value.

``` java
Query query = new Query.Builder(QueryType.SELECT_UNIQUE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isListResult()) {
	List<QueryResult> listResults = result.getListResults();
	for (QueryResult item : listResults) {
		if (item.isLong()) {
			// do something with long value
		}
	}
}
```

Specifying "Group By" in the query will cause the query response to be a GroupByResult object. This object stores Map<Group, QueryResult> objects, where the Group contains the unique property/value pairs.

``` java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withGroupBy("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
if (result.isGroupResult()) {
	for (Map.Entry<Group, QueryResult> groupResult : result.getGroupResults().entrySet()) {
	    Map<String, Object> groupProperies = groupResult.getKey().getProperties();
	    long groupCount = groupResult.getValue().longValue();
	    // ... do something with the group properties and the count result
	}
}
```
Specifying "Interval" in the query will cause the query response to be an IntervalResult object. An IntervalResult is a type of QueryResult that consist of Map<AbsoluteTimeframe,QueryResult> objects.

``` java
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);
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
Query query = new Query.Builder(QueryType.COUNT)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withGroupBy("click-number")
        .withTimeframe(new RelativeTimeframe("this_month"))
        .build();
QueryResult result = queryClient.execute(query);

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

// this will add two filter parameters, with 1 < click-count < 5
Query query = new Query.Builder(QueryType.COUNT)
	            .withEventCollection("<event_collection>")
	            .withFilter("click-count", FilterOperator.GREATER_THAN, 1)
	            .withFilter("click-count", FilterOperator.LESS_THAN, 5)
	            .withTimeframe(new RelativeTimeframe("this_month"))
	            .build();

QueryResult result = queryClient.execute(query);
if (result.isLong()) {
	long queryResult = result.longValue();
}
```


## Changelog

##### 1.0
Introduce KeenQueryClient.