# QueryApi

All URIs are relative to *http://localhost:8080/*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getDataSource**](QueryApi.md#getDataSource) | **POST** /api/stroom-stats/v2/dataSource | Get data source for a DocRef
[**search**](QueryApi.md#search) | **POST** /api/stroom-stats/v2/search | Execute a stats search


<a name="getDataSource"></a>
# **getDataSource**
> DataSource getDataSource(body)

Get data source for a DocRef



### Example
```java
// Import classes:
//import stroom.stats.ApiException;
//import stroom.stats.api.QueryApi;


QueryApi apiInstance = new QueryApi();
DocRef body = new DocRef(); // DocRef | docRef
try {
    DataSource result = apiInstance.getDataSource(body);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling QueryApi#getDataSource");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**DocRef**](DocRef.md)| docRef | [optional]

### Return type

[**DataSource**](DataSource.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="search"></a>
# **search**
> SearchResponse search(body)

Execute a stats search



### Example
```java
// Import classes:
//import stroom.stats.ApiException;
//import stroom.stats.api.QueryApi;


QueryApi apiInstance = new QueryApi();
SearchRequest body = new SearchRequest(); // SearchRequest | searchRequest
try {
    SearchResponse result = apiInstance.search(body);
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling QueryApi#search");
    e.printStackTrace();
}
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**SearchRequest**](SearchRequest.md)| searchRequest | [optional]

### Return type

[**SearchResponse**](SearchResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

