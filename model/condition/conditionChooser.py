from condition.Condition import DataCondition, TimeInstantCondition, SeriesCondition
from measures import base
from functionsFrame import auxFunctions

def conditionChooser(dataframe, id_case, condition):

    if(type(condition) == DataCondition):
        dataframeValue = dataframe.query(str(condition))
        filteredSeries = dataframe.index.isin(dataframeValue.index)

    if(type(condition) ==  base.TimeInstantCondition):
        filteredSeries = auxFunctions.timeInstantConditionFunction(dataframe, id_case, str(condition))
        
    if(type(condition) == SeriesCondition):
        filteredSeries = condition
  
    return filteredSeries