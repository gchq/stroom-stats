
# TimeZone

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**use** | [**UseEnum**](#UseEnum) | The required type of time zone | 
**id** | **String** | The id of the time zone, conforming to java.time.ZoneId |  [optional]
**offsetHours** | **Integer** | The number of hours this timezone is offset from UTC |  [optional]
**offsetMinutes** | **Integer** | The number of minutes this timezone is offset from UTC |  [optional]


<a name="UseEnum"></a>
## Enum: UseEnum
Name | Value
---- | -----
LOCAL | &quot;LOCAL&quot;
UTC | &quot;UTC&quot;
ID | &quot;ID&quot;
OFFSET | &quot;OFFSET&quot;



