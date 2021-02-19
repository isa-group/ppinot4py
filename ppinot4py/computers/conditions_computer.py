from ppinot4py.model import AppliesTo, RuntimeState, TimeInstantCondition
import pandas as pd

def condition_computer(dataframe, id_case, condition):

    if(type(condition) == str):
        dataframeValue = dataframe.query(condition)
        filteredArray = dataframe.index.isin(dataframeValue.index)
        filteredSeries = pd.Series(filteredArray)

    if(type(condition) == TimeInstantCondition):
        filteredSeries = _time_instant_condition_resolve(dataframe, id_case, condition)
        
    #In revision    
    if(type(condition) == pd.Series):
        filteredSeries = condition
  
    return filteredSeries



def _time_instant_condition_resolve(dataframe, id_case, condition: TimeInstantCondition):
    TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)
    if condition.applies_to == AppliesTo.DATA:
        return _time_instant_condition_data_resolve(dataframe, id_case, str(condition.changes_to_state))
    elif condition.applies_to == AppliesTo.PROCESS:
        return _time_instant_condition_process_resolve(dataframe, id_case, condition.changes_to_state)
    else:
        raise ValueError("invalid applies to condition " + condition.applies_to)

def _time_instant_condition_process_resolve(dataframe, id_case, changes_to_state):
    default_false = pd.Series(data=False, index=dataframe.index)

    if changes_to_state == RuntimeState.START:
        shift = +1
    else:
        shift = -1

    condition = default_false.groupby(dataframe[id_case]).shift(shift).fillna(True)
    return condition

def _time_instant_condition_data_resolve(dataframe, id_case, var):
    condition = dataframe.query(var)
    condition_in_series = pd.Series(dataframe.index.isin(condition.index))

    condition_in_series_prev = condition_in_series.groupby(dataframe[id_case]).shift(+1).fillna(False)

    final_evaluation = ((condition_in_series) & (~condition_in_series_prev))

    # id_next = dataframe[id_case].shift(-1)
    # id_pre = dataframe[id_case].shift(+1)
    # condition = dataframe.query(var)

    # is_last = (dataframe[id_case] != dataframe[id_case].shift(-1)).reset_index(drop=True)
    # prev_is_last = is_last.shift(+1).fillna(True)

    # dataframeValue = dataframe.index.isin(condition.index)

    # conditionInSeries = pd.Series(dataframeValue)
    # conditionInSeries_prev = conditionInSeries.shift(+1).fillna(False)

    # partial_evaluation_next = conditionInSeries.shift(+1).fillna(False)

    # final_evaluation = ((conditionInSeries) & ((~conditionInSeries_prev) | prev_is_last) )

    # final_evaluation = ((conditionInSeries == True) &
    #                         (partial_evaluation_next == False) & 
    #                             ((dataframe[id_case] == id_pre) | (dataframe[id_case] == id_next)))
    # I don't think this == id_next is ok. It doesn't meet the condition if the process instance
    # finishes

    return final_evaluation    