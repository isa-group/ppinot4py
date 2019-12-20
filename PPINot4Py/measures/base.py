class CountMeasure():
    """Measure to calc how many times a condition happened.
    
    Args:
    
            - when: Condition that you want to count
    """

    def __init__(self, when):
        self.when = when
    
    def __repr__(self):
        return "%s" % (self.when)

class DataMeasure():
    """Measure that returns a data from specific filter.
    
    Args:
    
            - dataContentSelection: Column you want to select
            - precondition: Filter to apply to data
            - first: True if you want to select first data, False if you want to select last
    """
    
    def __init__(self, dataContentSelection, precondition, first):
       
        self.dataContentSelection = dataContentSelection
        self.precondition = precondition
        self.first = first


class TimeMeasure():
    """Measure to calc the time elapsed between A condition and B condition.
    
    Args:
    
            - fromCondition: Condition where you want to start counting, defined as A
            - toCondition: End condition, defined as B
            - timeMeasureType: Linear in case just want a simple A-B calc, Cyclic in case you want to count 
                all A-B appearances
            - singleInstanceAggFunction: Only in Cyclic mode. Type of operation applied to our dataset, 
                can be MAX, MIN, MAX, AVG or GROUPBY. 
            - precondition: Filter to previusly apply to our dataset
            - firstTo: Only in Linear mode. True to take first appareance of B, false to take last appareance of B
    """

    def __init__(self, fromCondition, toCondition, 
                    timeMeasureType = 'LINEAR', singleInstanceAggFunction = 'SUM', 
                         firstTo = False, precondition = ''):
  
        self.fromCondition = fromCondition
        self.toCondition = toCondition
        self.timeMeasureType = timeMeasureType
        self.singleInstanceAggFunction = singleInstanceAggFunction
        self.precondition = precondition
        self.firstTo = firstTo


class aggregatedMeasure():
    """Measure to group a dataset by certain time interval.
    
    Args:
    
            - baseMeasure: First measure applied to the base dataframe.
            - filterToApply: Measure applied to our baseMeasure to filter specific values.
            - singleInstanceAggFunction: Type of operation applied to our dataset, 
                can be MAX, MIN, MAX, AVG or GROUPBY.
            - grouper: Time grouper (https://pandas.pydata.org/pandas-docs/stable/
                user_guide/timeseries.html#offset-aliases).
    """
    def __init__(self, baseMeasure, filterToApply, singleInstanceAggFunction, grouper):
  
        self.baseMeasure = baseMeasure
        self.filterToApply = filterToApply
        self.singleInstanceAggFunction = singleInstanceAggFunction
        self.grouper = grouper
        
 
class derivedMeasure():
    """Measure that applies an operation to a group of measures.
    
    Args:
    
            - functionExpression: Aritmathical or logical expresion as String
            - measureMap: Map of {nameOfMeasure: Measure}
    """
    
    def __init__(self, functionExpression, measureMap):
      
        self.functionExpression = functionExpression
        self.measureMap = measureMap
