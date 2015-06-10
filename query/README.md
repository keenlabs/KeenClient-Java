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
Users can use the KeenQueryClient to send queries as follows:
```java
Integer result = queryClient.count("<event_collection>");
```
Alternatively, users can use optional parameters via the object KeenQueryParams. However, because the result may be different depending on the optional parameters, the return value is an Object. The user is expected to verify the expected return type for the query, given the parameters entered.
```java
KeenQueryParams queryParams = new QueryParamBuilder()
        .withEventCollection("<event_collection>")
        .withRelativeTimeframe("this_month")
        .withGroupBy("click-number")
        .build();
Object result = queryClient.count(queryParams);
```
There are also some utility methods to add filters and absolute timeframes to KeenQueryParams:
```java
KeenQueryParams queryParams = new QueryParamBuilder()
	            .withEventCollection(TEST_EVENT_COLLECTION)
	            .build();

queryParams.addFilter("click-count", "lt", 5);
queryParams.addFilter("click-count", "gt", 1);
queryParams.addAbsoluteTimeframe("2012-08-13T19:00:00.000Z", "2015-06-07T19:00:00.000Z");

Object result = queryClient.count(queryParams);
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