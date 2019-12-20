from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base

def auxiliarLinearTimeMeasure(fromCondition, ToCondition, firstTo, precondition = ''):
    
    countStateTimeA = DataObjectState.DataObjectState(fromCondition)
    countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
    countMeasureTimeA = base.CountMeasure(countConditionTimeA)
        
    countStateTimeC = DataObjectState.DataObjectState(ToCondition)
    countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
    countMeasureTimeC = base.CountMeasure(countConditionTimeC)
        
    timeMeasureLinearA = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC, 'LINEAR', 'SUM', firstTo, precondition)
    
    return timeMeasureLinearA


def auxiliarCyclicTimeMeasure(fromCondition, ToCondition, operation, precondition = ''):
    
    countStateTimeA = DataObjectState.DataObjectState(fromCondition)
    countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
    countMeasureTimeA = base.CountMeasure(countConditionTimeA)
        
    countStateTimeC = DataObjectState.DataObjectState(ToCondition)
    countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
    countMeasureTimeC = base.CountMeasure(countConditionTimeC)
        
    timeMeasureCyclic = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC, 'CYCLIC', operation, True, precondition)
    
    return timeMeasureCyclic

def auxiliarCountMeasure(condition):
    
    countState = DataObjectState.DataObjectState(condition)
    countCondition = Condition.TimeInstantCondition(countState)
    countMeasure = base.CountMeasure(countCondition)
    
    return countMeasure

def auxiliarDataMeasure(condition, data, isFirst):
    
    countState = DataObjectState.DataObjectState(condition)
    countCondition = Condition.TimeInstantCondition(countState)
    countMeasure = base.CountMeasure(countCondition)
    
    dataMeasure = base.DataMeasure(data, countMeasure, isFirst)
    
    return dataMeasure