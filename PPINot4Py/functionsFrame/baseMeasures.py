from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base
from PPINot4Py import timeGrouper, importBase, computer

def linearTimeMeasure(fromCondition, ToCondition, firstTo, precondition = ''):
    
    countStateTimeA = DataObjectState.DataObjectState(fromCondition)
    countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
    countMeasureTimeA = base.CountMeasure(countConditionTimeA)
        
    countStateTimeC = DataObjectState.DataObjectState(ToCondition)
    countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
    countMeasureTimeC = base.CountMeasure(countConditionTimeC)
        
    timeMeasureLinearA = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC, 'LINEAR', 'SUM', firstTo, precondition)
    
    return timeMeasureLinearA


def cyclicTimeMeasure(fromCondition, ToCondition, operation, precondition = ''):
    
    countStateTimeA = DataObjectState.DataObjectState(fromCondition)
    countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
    countMeasureTimeA = base.CountMeasure(countConditionTimeA)
        
    countStateTimeC = DataObjectState.DataObjectState(ToCondition)
    countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
    countMeasureTimeC = base.CountMeasure(countConditionTimeC)
        
    timeMeasureCyclic = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC, 'CYCLIC', operation, True, precondition)
    
    return timeMeasureCyclic

def countMeasure(condition):
    
    countState = DataObjectState.DataObjectState(condition)
    countCondition = Condition.TimeInstantCondition(countState)
    countMeasure = base.CountMeasure(countCondition)
    
    return countMeasure

def dataMeasure(condition, data, isFirst):
    
    countState = DataObjectState.DataObjectState(condition)
    countCondition = Condition.TimeInstantCondition(countState)
    countMeasure = base.CountMeasure(countCondition)
    
    dataMeasure = base.DataMeasure(data, countMeasure, isFirst)
    
    return dataMeasure

def aggregatedMeasure(measure, frequency, operation, filter = ''):
    
    timeGrouperEx = timeGrouper.grouper(frequency)
    aggregatedMeasure = base.aggregatedMeasure(measure, '', operation, timeGrouperEx)
    
    return aggregatedMeasure

def derivedMeasure(measure_dictionary, function):
    
    derivedMeasure = base.derivedMeasure(function, measure_dictionary)
    
    return derivedMeasure