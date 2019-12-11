from PPINot4Py.condition.Condition import DataCondition, TimeInstantCondition, SeriesCondition
from PPINot4Py.measures import base
from PPINot4Py.functionsFrame import auxFunctions
import pandas as pd

def conditionChooser(dataframe, id_case, condition):

    if(type(condition) == str):
        dataframeValue = dataframe.query(condition)
        filteredArray = dataframe.index.isin(dataframeValue.index)
        filteredSeries = pd.Series(filteredArray)

    if(type(condition) == TimeInstantCondition or type(condition) == base.CountMeasure):
        filteredSeries = auxFunctions.timeInstantConditionFunction(dataframe, id_case, str(condition))
        
    #In revision    
    if(type(condition) == pd.Series):
        filteredSeries = condition
  
    return filteredSeries