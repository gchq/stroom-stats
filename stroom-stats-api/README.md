# stroom-stats-api

## Getting Started

```java

import stroom.stats.*;
import stroom.stats.auth.*;
import stroom.stats.api.model.*;
import stroom.stats.api.QueryApi;

import java.io.File;
import java.util.*;

public class QueryApiExample {

    public static void main(String[] args) {
        
        QueryApi apiInstance = new QueryApi();
        try {
            DataSource result = apiInstance.getDataSource();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling QueryApi#getDataSource");
            e.printStackTrace();
        }
    }
}

```

## Documentation for API Endpoints

All URIs are relative to *http://localhost:8080/*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*QueryApi* | [**getDataSource**](docs/QueryApi.md#getDataSource) | **POST** /api/stroom-stats/v2/dataSource | Get data source for a DocRef
*QueryApi* | [**search**](docs/QueryApi.md#search) | **POST** /api/stroom-stats/v2/search | Execute a stats search


## Documentation for Models

 - [DataSource](docs/DataSource.md)
 - [DataSourceField](docs/DataSourceField.md)
 - [DateTimeFormat](docs/DateTimeFormat.md)
 - [Field](docs/Field.md)
 - [Filter](docs/Filter.md)
 - [FlatResult](docs/FlatResult.md)
 - [Format](docs/Format.md)
 - [NumberFormat](docs/NumberFormat.md)
 - [OffsetRange](docs/OffsetRange.md)
 - [Result](docs/Result.md)
 - [Row](docs/Row.md)
 - [SearchResponse](docs/SearchResponse.md)
 - [Sort](docs/Sort.md)
 - [TableResult](docs/TableResult.md)
 - [TimeZone](docs/TimeZone.md)



