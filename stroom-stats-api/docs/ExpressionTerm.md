
# ExpressionTerm

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**field** | **String** | The name of the field that is being evaluated in this predicate term | 
**condition** | [**ConditionEnum**](#ConditionEnum) | The condition of the predicate term | 
**value** | **String** | The value that the field value is being evaluated against. Not required if a dictionary is supplied |  [optional]
**dictionary** | [**DocRef**](DocRef.md) | The DocRef for the dictionary that this predicate is using for its evaluation | 


<a name="ConditionEnum"></a>
## Enum: ConditionEnum
Name | Value
---- | -----
CONTAINS | &quot;CONTAINS&quot;
EQUALS | &quot;EQUALS&quot;
GREATER_THAN | &quot;GREATER_THAN&quot;
GREATER_THAN_OR_EQUAL_TO | &quot;GREATER_THAN_OR_EQUAL_TO&quot;
LESS_THAN | &quot;LESS_THAN&quot;
LESS_THAN_OR_EQUAL_TO | &quot;LESS_THAN_OR_EQUAL_TO&quot;
BETWEEN | &quot;BETWEEN&quot;
IN | &quot;IN&quot;
IN_DICTIONARY | &quot;IN_DICTIONARY&quot;



