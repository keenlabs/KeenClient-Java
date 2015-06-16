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
Optionally, users can also specify a HTTP Handler, base URL, or JSON Handler.:
```java
KeenQueryClient queryClient = new TestKeenQueryClientBuilder(queryProject)
	.withHttpHandler(httpHandler)
	.withJsonHandler(jsonHandler)
	.withBaseUrl(baseURL)
	.build();
```
### Using the KeenQueryClient to send Queries
The most simple way that users can use the KeenQueryClient to send queries is as follows. Please note that we strongly encourage users to pass in the Timeframe parameter, but it can be null.
```java
Integer count = queryClient.count("<event_collection>", new Timeframe("this_year"));
Integer countUnique = queryClient.countUnique("<event_collection>", "<target_property>", new Timeframe("this_year"));
Double minimum = queryClient.minimum("<event_collection>", "<target_property>", new Timeframe("this_year"));
```

### Advanced
Alternatively, users can use optional parameters via the object Query. However, because the result may be different depending on the optional parameters, the return value is a QueryResult. The user is expected to verify the expected return type for the query, given the parameters entered.
```java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isInteger()) {
	Integer countValue = result.getInteger();
	// do something with countValue
}
```
Some special cases are when filters "Group By" and "Inteval" are specified, as well as the Select Unique query.

Select Unique:
``` java
Query query = new QueryBuilder(QueryType.SELECT_UNIQUE_RESOURCE)
        .withEventCollection("<event_collection>")
        .withTargetProperty("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isList()) {
	ArrayList<QueryResult> listResults = result.getList();
	foreach (QueryResult item : listResults) {
		if (item.isInteger()) {
			// do something with Integer value
		}
	}
}
```


Group-By:
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_month"));
if (result.isList()) {
	ArrayList<QueryResult> listResults = result.getList();
	foreach (QueryResult item : listResults) {
		if (item.isGroupBy()) {
			GroupBy groupBy = item.getGroupBy();
			HashMap<String, QueryResult> properties = groupBy.getProperties();
			QueryResult groupByValue = groupBy.getValue();
			if (groupByValue.isInteger()) {
				// do something with integer result.
			}
		}
	}
}
```
Interval:
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_year"));
if (result.isList()) {
	ArrayList<QueryResult> listResults = result.getList();
	foreach (QueryResult item : listResults) {
		if (item.isInterval()) {
			Interval interval = item.getInterval();
			Timeframe itemTimeframe = interval.getTimeframe();
			QueryResult intervalValue = interval.getValue();
			if (intervalValue.isInteger()) {
				// do something with integer result.
			}
		}
	}
}        
```
Group By and Interval:
``` java
Query query = new QueryBuilder(QueryType.COUNT_RESOURCE)
        .withEventCollection("<event_collection>")
        .withInterval("weekly")
        .withGroupBy("click-number")
        .build();
QueryResult result = queryClient.execute(query, new Timeframe("this_year"));
if (result.isList()) {
	ArrayList<QueryResult> listResults = result.getList();
	foreach (QueryResult item : listResults) {
		Interval interval = item.getInterval();
		Timeframe itemTimeframe = interval.getTimeframe();
		QueryResult intervalValue = interval.getValue();
		if (intervalValue.isGroupBy()) {
			GroupBy groupBy = intervalValue.getGroupBy();
			HashMap<String, QueryResult> properties = groupBy.getProperties();
			QueryResult groupByValue = groupBy.getValue();
			if (groupByValue.isInteger()) {
				// do something with integer result.
			}
		}
	}
}
```


There are also some utility methods to add filters and absolute timeframes to Query:
```java
Query query = new QueryBuilder()
	            .withEventCollection(TEST_EVENT_COLLECTION)
	            .build();

query.addFilter("click-count", "lt", 5);
query.addFilter("click-count", "gt", 1);
query.addAbsoluteTimeframe("2012-08-13T19:00:00.000Z", "2015-06-07T19:00:00.000Z");

Object result = queryClient.count(query);
Integer queryResult = null;
if (result instance Integer) {
	queryResult = (Integer)result;
}
```
Special queries such as Funnel and Multi-analysis are also supported, although the user is responsible of constructing her own JSON map for steps and multi-analysis, respectively:
Funnel:
```java
// construct the list of steps
List<Map<String, Object>> listSteps = new ArrayList<Map<String, Object>>();
Map<String, Object> steps = new HashMap<String, Object>();
steps.put(KeenQueryConstants.ACTOR_PROPERTY, "click-count");
steps.put(KeenQueryConstants.EVENT_COLLECTION, "<event collection>");
listSteps.add(steps);

// run query
Object result = queryClient.funnel(listSteps);
```
Multi-Analysis:
```java
// create analyses object
Map<String, Object> analyses = new HashMap<String, Object>();

// add a first set for Count Query.
Map<String, String> firstSet = new HashMap<String, String>();
firstSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.COUNT_RESOURCE);
analyses.put("count set", firstSet);

// add a second set for Sum Resource Query.
Map<String, String> secondSet = new HashMap<String, String>();
secondSet.put(KeenQueryConstants.ANALYSIS_TYPE, KeenQueryConstants.SUM_RESOURCE);
secondSet.put(KeenQueryConstants.TARGET_PROPERTY, "click-count");
analyses.put("sum set", secondSet);

Object result = queryClient.multiAnalysis("<event collection>", analyses);

```


## Changelog

##### 1.0
Introduce KeenQueryClient.