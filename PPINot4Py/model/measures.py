from .conditions import TimeInstantCondition

def _time_instance_auto_wrap(condition):
    if isinstance(condition, str):
        condition = TimeInstantCondition(condition)
    
    return condition


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

class DataMeasure(_MeasureDefinition):
    """Measure that returns a data from specific filter.
    
    Args:
    
            - data_content_selection: Column you want to select
            - precondition: Filter to apply to data
            - first: True if you want to select first data, False if you want to select last
    """
    
    def __init__(self, data_content_selection, precondition, first):
        super().__init__()
       
        self.data_content_selection = data_content_selection
        self.precondition = _time_instance_auto_wrap(precondition)
        self.first = first


class TimeMeasure(_MeasureDefinition):
    """Measure to calc the time elapsed between A condition and B condition.
    
    Args:
    
            - from_condition: Condition where you want to start counting, defined as A
            - to_condition: End condition, defined as B
            - time_measure_type: Linear in case just want a simple A-B calc, Cyclic in case you want to count 
                all A-B appearances
            - single_instance_agg_function: Only in Cyclic mode. Type of operation applied to our dataset, 
                can be MAX, MIN, SUM, or AVG. 
            - first_to: Only in Linear mode. True to take first appareance of B, false to take last appareance of B
            - precondition: Filter to previusly apply to our dataset
    """

    def __init__(self, from_condition, to_condition, 
                    time_measure_type = 'LINEAR', single_instance_agg_function = 'SUM', 
                         first_to = False, precondition = ''):
        super().__init__()
  
        self.from_condition = _time_instance_auto_wrap(from_condition)
        self.to_condition = _time_instance_auto_wrap(to_condition)
        self.time_measure_type = time_measure_type
        self.single_instance_agg_function = single_instance_agg_function
        self.precondition = precondition
        self.first_to = first_to

    def __repr__(self):
        return f"TimeMeasure ( from={self.from_condition}, to={self.to_condition} )"


class AggregatedMeasure(_MeasureDefinition):
    """Measure to group a dataset by certain time interval.
    
    Args:
    
            - base_measure: First measure applied to the base dataframe.
            - single_instance_agg_function: Type of operation applied to our dataset, 
                can be MAX, MIN, SUM, AVG or GROUPBY.
            - grouper: Array of measures for grouping.
            - filter_to_apply: Measure applied to our base_measure to filter specific values.
    """
    def __init__(self, base_measure, single_instance_agg_function, grouper=None, filter_to_apply=None):
        super().__init__()
  
        self.base_measure = base_measure
        self.filter_to_apply = filter_to_apply
        self.single_instance_agg_function = single_instance_agg_function
        if grouper is None:
            self.grouper = []
        else:
            self.grouper = grouper
        
 
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
