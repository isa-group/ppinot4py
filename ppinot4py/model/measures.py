from .conditions import TimeInstantCondition
import datetime
import enum

def _time_instance_auto_wrap(condition):
    if condition is None:
        return None

    if isinstance(condition, str):
        condition = TimeInstantCondition(condition)
    
    return condition

def _name_of_aggregation(agg):
    if agg == "AVG":
        return "average"
    if agg == "MIN":
        return "minimum"
    if agg == "MAX":
        return "maximum"
    if agg == "SUM":
        return "sum"

class TimeUnit(enum.Enum):
    YEAR = "Y"
    MONTH = "M"
    DAY = "D"
    HOUR = "h"
    MINUTE = "m"
    SECOND = "s"

class _MeasureDefinition():
    def __init__(self):
        super().__init__()
        self.id = None

class CountMeasure(_MeasureDefinition):
    """Measure to calc how many times a condition happened.
    
    Args:
    
            - when: Condition that you want to count
    """

    def __init__(self, when):
        super().__init__()
        self.when = _time_instance_auto_wrap(when)
    
    def __repr__(self):
        return f"CountMeasure ( {self.when} )"

    def __str__(self):
        return f"the number of times {self.when}"

class DataMeasure(_MeasureDefinition):
    """Measure that returns a data from specific filter.
    
    Args:
    
            - data_content_selection: Column you want to select
            - precondition: Condition (Time Instance Condition by default) to apply to data
            - first: True if you want to select first data, False if you want to select last
    """
    
    def __init__(self, data_content_selection, precondition=None, first=False):
        super().__init__()
       
        self.data_content_selection = data_content_selection
        self.precondition = _time_instance_auto_wrap(precondition)
        self.first = first

    def __str__(self):
        first_text = "first" if self.first else "last"
        precondition_text = f" when {self.precondition}" if self.precondition is not None else ""
        return f"the {first_text} value of {self.data_content_selection}{precondition_text}"

class TimeMeasure(_MeasureDefinition):
    """Measure to calc the time elapsed between A condition and B condition.
    
    Args:
    
        - from_condition: Condition where you want to start counting, defined as A
        - to_condition: End condition, defined as B
        - time_measure_type: Linear in case just want a simple A-B calc, Cyclic in case you want to count 
            all pairs A-B appearances
        - single_instance_agg_function: Only in Cyclic mode. Type of operation applied to our dataset, 
            can be MAX, MIN, SUM, or AVG. 
        - first_to: Only in Linear mode. True to take first appareance of B, false to take last appareance of B
        - precondition: Filter to previusly apply to our dataset. Note that, in this case, this precondition
                        is not a time instant condition, but a data condition (i.e., it is directly checked with
                        the values of the attributes of each event)
        - business_duration: Business days and hours specification
        - time_unit: The time unit used to return the value.
    """

    def __init__(self, from_condition, to_condition, 
                    time_measure_type = 'LINEAR', single_instance_agg_function = 'SUM', 
                         first_to = False, precondition = None, business_duration = None, time_unit = None):
        super().__init__()
  
        self.from_condition = _time_instance_auto_wrap(from_condition)
        self.to_condition = _time_instance_auto_wrap(to_condition)
        self.time_measure_type = time_measure_type
        self.single_instance_agg_function = single_instance_agg_function
        self.precondition = precondition
        self.first_to = first_to
        self.business_duration = business_duration
        self.time_unit = time_unit
        
    def __repr__(self):
        return f"TimeMeasure ( from={self.from_condition}, to={self.to_condition} )"

    def __str__(self):
        precon_text = f" if {self.precondition}" if self.precondition is not None else ""
        first_to_text = "first" if self.first_to else "last"

        if self.time_measure_type.upper() == 'LINEAR':
            text = f"the duration between the first time instant when {self.from_condition} and the {first_to_text} time instant when {self.to_condition}{precon_text}"
        else:
            agg_text = _name_of_aggregation(self.single_instance_agg_function)
            text = f"the {agg_text} duration between the pairs of time instants when {self.from_condition} and when {self.to_condition}{precon_text}"
        return text


class AggregatedMeasure(_MeasureDefinition):
    """Measure to group a dataset by certain time interval.
    
    Args:
    
            - base_measure: First measure applied to the base dataframe.
            - single_instance_agg_function: Type of operation applied to our dataset, 
                can be MAX, MIN, SUM, AVG or GROUPBY.
            - grouper: Array of measures for grouping.
            - filter_to_apply: Measure applied to our base_measure to filter specific values. The measure
                               must resolve to either true or false.
    """
    def __init__(self, base_measure, single_instance_agg_function, grouper=None, filter_to_apply=None, period_reference_point=None):
        super().__init__()
  
        self.base_measure = base_measure
        self.filter_to_apply = filter_to_apply
        self.single_instance_agg_function = single_instance_agg_function
        self.period_reference_point = period_reference_point
        if grouper is None:
            self.grouper = []
        elif isinstance(grouper, _MeasureDefinition):
            self.grouper = [grouper]
        else:
            self.grouper = grouper

    def __str__(self):
        result =  f"the {_name_of_aggregation(self.single_instance_agg_function)} of {self.base_measure}"
        if len(self.grouper) > 0:
            grouper_text = ", ".join([f"{g}" for g in self.grouper])
            result = f"{result} grouped by {grouper_text}"
        if self.filter_to_apply is not None:
            result = f"{result} filtered by {self.filter_to_apply}"

        return result
        
 
class DerivedMeasure(_MeasureDefinition):
    """Measure that applies an operation to a group of measures.
    
    Args:
    
            - function_expression: Aritmathical or logical expresion as String
            - measure_map: Map of {name_of_measure: Measure}
    """
    
    def __init__(self, function_expression, measure_map):
        super().__init__()
      
        self.function_expression = function_expression
        self.measure_map = measure_map

    def __str__(self):
        variables = ", ".join([f"{m} is {self.measure_map[m]}" for m in self.measure_map])

        return f"the function {self.function_expression} where {variables}"

class BusinessDuration():
    
    def __init__(self, business_start, business_end, weekend_list=[5,6], holiday_list=None, unit_hour='min'):
        super().__init__()
        
        self.business_start = business_start
        self.business_end = business_end
        self.weekend_list = weekend_list
        self.holiday_list = holiday_list
        self.unit_hour = unit_hour

    def conversion(self):
        if(self.unit_hour == 'day'):
            time_delta_type = lambda x: (datetime.timedelta(days = x))
        elif(self.unit_hour == 'hour'):
            time_delta_type = lambda x: (datetime.timedelta(hours = x))
        elif(self.unit_hour == 'min'):
            time_delta_type = lambda x: (datetime.timedelta(minutes = x))
        elif(self.unit_hour == 'sec'):
            time_delta_type = lambda x: (datetime.timedelta(seconds = x))

        return time_delta_type

class RollingWindow():
    """Window applied to the data to group with a moving window.
    
    Args:
        - window: Size of the moving window, could be a number or a time expression ('7D'). If it is a number, then apply_to_cases must be true.
        - apply_to_cases: Returns the moving window for each case instead of based on the window frequency.
        - min_period: Minimum number of observations in window required to have a value 
        - closed: Make the interval closed on the ‘right’, ‘left’, ‘both’ or ‘neither’ endpoints.
    """
    def __init__(self, window, apply_to_cases=False, min_period=None, closed=None):
        if (isinstance(window, int) or (isinstance(window,float) and window.is_integer())) and not apply_to_cases:
            raise ValueError('If window is an integer, apply_to_cases must be True')

        self.window = window
        self.apply_to_cases = apply_to_cases
        self.min_period = min_period
        self.closed = closed



 
        
