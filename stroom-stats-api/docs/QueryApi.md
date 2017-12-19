# QueryApi

All URIs are relative to *http://localhost:8080/*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getDataSource**](QueryApi.md#getDataSource) | **POST** /api/stroom-stats/v2/dataSource | Get data source for a DocRef
[**search**](QueryApi.md#search) | **POST** /api/stroom-stats/v2/search | Execute a stats search


<a name="getDataSource"></a>
# **getDataSource**
> DataSource getDataSource()

Get data source for a DocRef



### Example
```java
// Import classes:
//import stroom.stats.ApiException;
//import stroom.stats.api.QueryApi;


QueryApi apiInstance = new QueryApi();
try {
    DataSource result = apiInstance.getDataSource();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling QueryApi#getDataSource");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**DataSource**](DataSource.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

<a name="search"></a>
# **search**
> SearchResponse search()

Execute a stats search



### Example
```java
// Import classes:
//import stroom.stats.ApiException;
//import stroom.stats.api.QueryApi;


QueryApi apiInstance = new QueryApi();
try {
    SearchResponse result = apiInstance.search();
    System.out.println(result);
} catch (ApiException e) {
    System.err.println("Exception when calling QueryApi#search");
    e.printStackTrace();
}
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**SearchResponse**](SearchResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

