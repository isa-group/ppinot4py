# What is PPINot4Py?

PPINot4Py is a Python implementation of a [PPINot](https://github.com/isa-group/ppinot), used to compute process performance indicators (PPIs) for event log datasets.

* **A quick example**

In the following example, we use the [Road Traffic Fine event log](https://data.4tu.nl/articles/dataset/Road_Traffic_Fine_Management_Process/12683249) to show how ppinot4py can be used to compute some PPIs. The log is in XES format, so we use [pm4py](http://pm4py.fit.fraunhofer.de/) to load it into a dataframe:

``` python
import ppinot4py
from ppinot4py import model
import pandas as pd
import pm4py
from pm4py.objects.conversion.log import converter as log_converter

# Loads the event log
log = pm4py.read_xes('Road_Traffic_Fine_Management_Process.xes')

# Transforms the event log into a pandas dataframe
df = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)

# Converts the timestamp column into a timestamp
df['time:timestamp'] = pd.to_datetime(df['time:timestamp'], utc=True)

# Computes the time between activity Create Fine and activity Send Fine
tm = model.TimeMeasure('`concept:name` == "Create Fine"', '`concept:name` == "Send Fine"')
result = ppinot4py.measure_computer(tm, df)
```

The value of result is:

``` python
id
A1       134 days 01:00:00
A100     132 days 01:00:00
A10000   129 days 23:00:00
A10001   119 days 23:00:00
A10004   118 days 23:00:00
                ...       
V9995     48 days 00:00:00
V9996     48 days 00:00:00
V9997     48 days 00:00:00
V9998     48 days 00:00:00
V9999     48 days 00:00:00
Name: t, Length: 150370, dtype: timedelta64[ns]
```

We can also execute more complex metrics. For instance, we can get the percentage of cases in which the time between Create Fine and Send Fine is less than 90 days yearly grouped as follows:

``` python
create_to_send_fine_90_days= model.DerivedMeasure("create_to_send_fine < days90", 
                                {"create_to_send_fine": tm, "days90": pd.Timedelta(days=90)})
avg_create_to_send_fine_90_days = model.AggregatedMeasure(create_to_send_fine_90_days, 'avg')
ppinot4py.measure_computer(avg_create_to_send_fine_90_days, df, time_grouper=pd.Grouper(freq='1Y'))
```

The result is:

``` python
	          data
case_end	
2000-12-31 00:00:00+00:00	0.370000
2001-12-31 00:00:00+00:00	0.488830
2002-12-31 00:00:00+00:00	0.781479
2003-12-31 00:00:00+00:00	0.567777
2004-12-31 00:00:00+00:00	0.401980
2005-12-31 00:00:00+00:00	0.016107
2006-12-31 00:00:00+00:00	0.087001
2007-12-31 00:00:00+00:00	0.062628
2008-12-31 00:00:00+00:00	0.254578
2009-12-31 00:00:00+00:00	0.178580
2010-12-31 00:00:00+00:00	0.367412
2011-12-31 00:00:00+00:00	0.356082
2012-12-31 00:00:00+00:00	0.460812
2013-12-31 00:00:00+00:00	0.829418
```

This is just a small example of what can be done with ppinot4py. Next, you can find the details on how to use it.

* **Basic imports to use the library:**
``` python
from ppinot4py.model import * #To define the measure model
from ppinot4py import measure_computer #To perform the calculations
```


## Conditions

Measures need conditions to specify when to count or when to start or stop measuring time. In ppinot4py, you can specify these conditions in 2 different ways.

**1.- TimeInstant Condition:**
```python
countStateCount = DataObjectState("`concept:name` == 'Close'")
countConditionCount = TimeInstantCondition(countStateCount)
countMeasureCount = CountMeasure(countConditionCount)
```
or simply

```python
countMeasureExample = CountMeasure('`concept:name` == "Close"')
```

A TimeInstantCondition is True when the conditions changes in the event log from (!condition) -> (condition), so if our condition is "`concept:name`==A", and we have this secuence: A B A A A, the result will be True, False, True, False False.

The expression language that can be used to specify the condition is the same that can be used in [pandas DataFrame.query()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html).

**2.- Series Condition**
It is also possible to directly give the program a pandas Series with the calculated Boolean values.

## Measure computer

The measure computer function receives 5 paramethers, 3 of them are optional:

```python
def measure_computer(measure, dataframe, 
                    id_case = 'case:concept:name', 
                    time_column = 'time:timestamp',
                    time_grouper = None):
```
By default, _id_case_ and _time_column_ will have the standard name for those columns as specified by the XES standard. In case the user have custom names for these columns, they must be indicated.

Time grouper is a [pandas Grouper object](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases) that indicates how to group the results of an aggregated measure based on the time each case finishes. 

## Measures 

### Count Measure:

A count measure is composed of an unique attribute "When" that can be a String or a TimeInstantCondition and refers to the condition we want to evaluate.
``` python
class CountMeasure():

    def __init__(self, when):
        self.when = when
```
With this computer we will be abble to count how many times occurs in each ID of our dataframe the condition.

Example: For a certain dataset and the following condition:

``` python
countState = DataObjectState('concept:name == "In Progress"')
countCondition = TimeInstantCondition(countState)
countMeasure = CountMeasure(countCondition)

measure_computer(countMeasure, dataframe)
```
We obtain:
``` python
case_concept_name
1-364285768     7.0
1-467153946    16.0
1-503573772     7.0
1-504538555     8.0
1-506071646    28.0
               ...
1-740865953     2.0
1-740865969     2.0
1-740866691     1.0
1-740866708     1.0
1-740866821     0.0
Length: 7554, dtype: float64
```

### Data measure

A Data Measure is composed of 3 values:
* data_content_selection: Column you want to select.
* Precondition: Condition you want to apply to the dataset, can be TimeInstantCondition, Series or String.
* First: Boolean value, if is true, it will take the first filtered value of each ID, if is false, will take the last value.

``` python
class DataMeasure():
    
    def __init__(self, data_content_selection, precondition, first):
       
        self.data_content_selection = data_content_selection
        self.precondition = precondition
        self.first = first
```

With this measure type, you will be able to obtain a specific value of your event log for each ID.

Example: For a certain dataset and the following condition:

``` python
countState = DataObjectState("org:group == 'V5 3rd'")
precondition = TimeInstantCondition(countState)
dataMeasure = DataMeasure("lifecycle:transition", precondition, True)

measure_computer(dataMeasure, dataframe)
```
We obtain the value of lifecycle:transition for those cases where the precondition is met:

``` python
case_concept_name
1-364285768    Awaiting Assignment
1-692918254            In Progress
Name: lifecycle:transition, dtype: object
```

### Time measure

A Time Measure is composed of 6 attributes:

* **from_condition:** The starter condition where we want to count we will reffer to it as '_A_', it can be a TimeInstantCondition, a Series or a String
* **to_condition:** The final condition, we will refer to ir as '_B_' it can be a TimeInstantCondition, a Series or a String
* **time_measure_type:** Linear or Cyclic. By default is Linear
  * **Linear:** Count the time elapsed between the first _A_ and the last _B_
  * **Cyclic:** Count the time elapsed between all pairs of _A_ and _B_
* **single_instance_agg_function:** Type of operation we want to apply to our data, it only works with Cyclic Measure. By default is SUM. There are 5 tipes of operations:
  * **SUM:** The sum of all _A_ to _B_ pairs
  * **MIN:** Minimum time value between the _A_ to _B_ pairs
  * **MAX:** Maximum time value between the _A_ to _B_ pairs
  * **AVG:** The average time between all _A_ to _B_ pairs
  * **GROUPBY:** Raw grouped dataframe with no operation applied
* **first_to:** Only works with Linear measure and it indicates if we want to take the first occurrence of 'B' condition or the last. By default is False
* **precondition:** Condition applied before the calculation of A and B.
``` python
class TimeMeasure():

    def __init__(self, from_condition, to_condition, 
                    time_measure_type = 'Linear', single_instance_agg_function = 'SUM', 
                         first_to = 'False', precondition = ''):
  
        self.from_condition = from_condition
        self.to_condition = to_condition
        self.time_measure_type = time_measure_type
        self.single_instance_agg_function = single_instance_agg_function
        self.precondition = precondition
        self.first_to = first_to
```
**In this Linear example, we want to calculate how much time has passed between 'In progress' and the last 'Closed'**
``` python
state_A = DataObjectState('lifecycle:transition == "In Progress"')
condition_A = TimeInstantCondition(state_A)

state_B = DataObjectState('lifecycle:transition == "Closed"')
condition_B = TimeInstantCondition(state_B)

time_measure_linear = TimeMeasure(condition_A, condition_B)

measure_computer(time_measure_linear, dataframe)
```
``` ruby
case_concept_name
1-364285768   771 days 08:26:33 
1-467153946   477 days 13:10:03
1-512795200   401 days 08:29:23
1-537219938   318 days 12:45:49
1-543979253   292 days 14:10:21
                     ...
1-740861371     2 days 18:28:50
1-740862061     0 days 01:45:07
1-740862080     9 days 23:18:50
1-740865953     3 days 02:17:03
1-740865969     3 days 02:13:18
Name: data, Length: 4904, dtype: timedelta64[ns]
```

**In this Cyclic example, we want to calculate the average time of all pairs 'In Progress' - 'Awaiting Assignment' along all Ids:**

``` python
state_A = DataObjectState.DataObjectState('lifecycle:transition == "In Progress"')
condition_A = TimeInstantCondition(state_A)

to_state_C = DataObjectState('lifecycle:transition == "Awaiting Assignment"')
condition_C = TimeInstantCondition(to_state_C)

time_measure_cyclic = TimeMeasure(condition_A, condition_C, 'CYCLIC', 'AVG')

measure_computer(time_measure_cyclic, dataframe)
```
``` ruby
case_concept_name
1-364285768    0 days 00:12:02.250000
1-467153946   38 days 21:55:53.666667
1-503573772           3 days 21:29:36
1-504538555           1 days 01:46:43
1-506071646    6 days 06:51:22.583333
                        ...
1-740859781    0 days 03:17:48.333333
1-740862061           0 days 00:05:59
1-740862080           0 days 00:03:42
1-740865953           0 days 00:02:16
1-740865969           0 days 00:01:23
Name: data, Length: 3669, dtype: timedelta64[ns]
```

### Aggregated measure

An Aggregated Measure is composed of:

* **base_measure:** Can be any kind of the previous measures (Time, Count or Data)
* **single_instance_agg_function:** Operation we want to apply to data of each Time aggrupation
  * **SUM:** Sum of all values
  * **MIN:** Minimum value
  * **MAX:** Maximum value
  * **AVG:** Average of all values
  * **GROUPBY:** Raw grouped dataframe with no operation applied
* **data_grouper:** List of Measures to group by the base measure.
* **filter_to_apply:** Filter to apply to the base_measure, can be TimeInstantCondition, Series or String

``` python
class AggregatedMeasure():

    def __init__(self, base_measure, single_instance_agg_function, data_grouper, filter_to_apply):
  
        self.base_measure = base_measure
        self.filter_to_apply = filter_to_apply
        self.single_instance_agg_function = single_instance_agg_function
        self.data_grouper = data_grouper
```

In this Computer, we take the result of a previous and group it by time. This time is take as the last TimeStamp of each ID.

**We will take a Linear condition between 'In Progress' and 'Closed' and sum the values each 60 seconds**

``` python
import pandas as pd

state_A = DataObjectState('lifecycle:transition == "In Progress"')
condition_A = TimeInstantCondition(state_A)

state_B = DataObjectState('lifecycle:transition == "Closed"')
condition_B = TimeInstantCondition(state_B)

time_measure = TimeMeasure(condition_A, condition_B)

time_grouper_60s = pd.Grouper(freq='60s')
aggregated_measure = AggregatedMeasure(time_measure, 'SUM')

measure_computer(aggregated_measure, dataframe, time_grouper=time_grouper_60s)
```
``` ruby
time_to_calculate
2012-05-01 05:58:00+00:00     18 days 05:59:56
2012-05-01 05:59:00+00:00      0 days 00:00:00
2012-05-01 06:00:00+00:00      0 days 00:00:00
2012-05-01 06:01:00+00:00      0 days 00:00:00
2012-05-01 06:02:00+00:00      0 days 00:00:00
                                   ...
2012-05-22 23:18:00+00:00      0 days 00:00:00
2012-05-22 23:19:00+00:00    213 days 05:00:36
2012-05-22 23:20:00+00:00    947 days 00:30:03
2012-05-22 23:21:00+00:00    437 days 23:18:16
2012-05-22 23:22:00+00:00   1233 days 22:43:00
Freq: 60S, Name: data_seconds, Length: 31285, dtype: timedelta64[ns]
```

**We can group it for example in intervals of 2 weeks**
``` python
aggregated_measure = AggregatedMeasure(time_measure, 'SUM')

measure_computer(aggregated_measure, dataframe, time_grouper=pd.Grouper(freq='2W'))
```
``` ruby
time_to_calculate
2012-05-06 00:00:00+00:00    6554 days 17:33:54
2012-05-20 00:00:00+00:00   53639 days 01:32:15
2012-06-03 00:00:00+00:00    6794 days 05:15:30
Freq: 2W-SUN, Name: data_seconds, dtype: timedelta64[ns]
```

### Derived measure

A Derived Measure is composed of 2 attributes:

* **function_expression:** Function that we want to apply to some measures. Can be arithmetical or boolean
  * Example: (A + B) / C where A,B and C are the result of a previous computer
* **measure_map:** A dictionary where the Key values are the name we want to assign to that measure, and the values the measure

``` python
class DerivedMeasure():
    
    def __init__(self, function_expression, measure_map):
      
        self.function_expression = function_expression
        self.measure_map = measure_map
```
With this Computer, we will be able to apply arithmetical of boolean functions to a group of Computer results.

We define 3 Linear Measures, create the dictionary and then we define the function
``` python
time_measure_A = TimeMeasure(condition_A, condition_B)
time_measure_B = TimeMeasure(condition_B, condition_A)
time_measure_C = TimeMeasure(condition_A, condition_C)

measure_dictionary = 
      {'A': time_measure_A, 'B': time_measure_B, 'C': time_measure_C}

derived_measure = DerivedMeasure('(A + B) / C', measure_dictionary)

measure_computer(derived_measure, dataframe)
```
``` ruby
case_concept_name
1-364285768   0 days 06:41:34.285249
1-467153946   1 days 04:26:22.637717
1-512795200   0 days 23:40:01.383292
1-537219938   0 days 08:40:38.534620
1-543979253   0 days 13:33:22.743243
                       ...
1-740861371          0 days 00:00:00
1-740862061   0 days 00:00:17.568245
1-740862080   0 days 01:04:40.765766
1-740865953   0 days 00:32:46.345588
1-740865969   0 days 00:53:39.253012
Length: 4904, dtype: timedelta64[ns]
```

One can also use derived measures to define boolean expressions. For instance, we can define a boolean derived measure that returns true when the number of Send Fine activities in a case is greater or equal than 1.

``` python
has_send_fine = model.DerivedMeasure('count_send_fine >= 1',
    {"count_send_fine": model.CountMeasure('`concept:name` == "Send Fine"')})
ppinot4py.measure_computer(has_send_fine, df)
```

If the data type used in the comparison is an object, then it has to be added as a parameter in the expression and in the measure map. For instance, in the following example, we define _days90_ with the value _pd.Timedelta(days=90)_:

``` python
create_fine_to_send_fine = model.TimeMeasure('`concept:name` == "Create Fine"', '`concept:name` == "Send Fine"')
create_to_send_fine_90_days= model.DerivedMeasure("create_to_send_fine < days90", 
                                    {"create_to_send_fine":   create_fine_to_send_fine, "days90": pd.Timedelta(days=90)})
ppinot4py.measure_computer(avg_create_to_send_fine_90_days, df, time_grouper=pd.Grouper(freq='1Y'))
```