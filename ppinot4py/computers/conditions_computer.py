from numpy import true_divide
from ppinot4py.model import AppliesTo, RuntimeState, TimeInstantCondition, ComplexState, Type, DataObjectState
import pandas as pd


def condition_computer(dataframe: pd.DataFrame, id_case, condition: str | TimeInstantCondition | pd.Series | None, activity_column: str, transition_column: str):

    if condition is None:
        filtered_series = dataframe
        
    if(type(condition) == str):
        dataframe_value = dataframe.query(condition)
        filtered_array = dataframe.index.isin(dataframe_value.index)
        filtered_series = pd.Series(filtered_array)

    elif(type(condition) == TimeInstantCondition):
        filtered_series = _time_instant_condition_resolve(dataframe, id_case, condition, activity_column, transition_column)
        
    #In revision    
    elif(type(condition) == pd.Series):
        filtered_series = condition

    else:
        filtered_series = dataframe
  
    return filtered_series


def _time_instant_condition_resolve(dataframe, id_case, condition: TimeInstantCondition, activity_column, transition_column):

    if condition.applies_to == AppliesTo.DATA:
        if isinstance(condition.changes_to_state, ComplexState):
            raw_condition = _complex_state_condition_resolve(dataframe, id_case, condition.changes_to_state)
        else:
            raw_condition = _data_state_condition_resolve(dataframe, _condition_to_query_expression(condition.changes_to_state))
        return _apply_time_instant_semantics(raw_condition, dataframe[id_case])
    
    elif condition.applies_to == AppliesTo.PROCESS:
        return _time_instant_condition_process_resolve(dataframe, id_case, condition.changes_to_state)
    
    elif condition.applies_to == AppliesTo.ACTIVITY:
        condition_checked = _check_state(condition.changes_to_state)
        if(_check_column_existence(dataframe, activity_column, transition_column)):
            data_condition = f'`{activity_column}` == {condition.activity_name} and `{transition_column}` == {condition_checked}'
            raw_condition = _data_state_condition_resolve(dataframe, data_condition)
            return _apply_time_instant_semantics(raw_condition, dataframe[id_case])
        elif(_check_end_condition(condition.changes_to_state)):
            data_condition = f'`{activity_column}` == {condition.activity_name}'
            raw_condition = _data_state_condition_resolve(dataframe, data_condition)
            return _apply_time_instant_semantics(raw_condition, dataframe[id_case])
        else: 
            raise ValueError("The activity or transition column don't exists in log")
    else:
        raise ValueError("invalid applies to condition " + str(condition.applies_to))

def _time_instant_condition_process_resolve(dataframe, id_case, changes_to_state):
    default_false = pd.Series(data=False, index=dataframe.index)

    if changes_to_state == RuntimeState.START:
        shift = +1
    else:
        shift = -1

    condition = default_false.groupby(dataframe[id_case]).shift(shift).fillna(True).astype(bool)
    return condition

def _data_state_condition_resolve(dataframe, var):
    condition = dataframe.query(var)
    return pd.Series(dataframe.index.isin(condition.index))

def _complex_state_condition_resolve(dataframe, id_case, complex_state):
    first_series = _data_state_condition_resolve(dataframe, _condition_to_query_expression(complex_state.first))
    last_series = _data_state_condition_resolve(dataframe, _condition_to_query_expression(complex_state.last))

    if complex_state.type == Type.FOLLOWS:
        first_before_last = first_series.groupby(dataframe[id_case]).shift(+1).fillna(False).astype(bool)
    elif complex_state.type == Type.LEADSTO:
        first_before_last = _eventual_with_reset(first_series, last_series, dataframe[id_case])
    else:
        raise ValueError(f"ComplexState type not valid: {complex_state.type}")

    return last_series & first_before_last

def _apply_time_instant_semantics(raw_condition, case_series):
    condition_in_series_prev = raw_condition.groupby(case_series).shift(+1).fillna(False).astype(bool)
    return raw_condition & (~condition_in_series_prev)

def _eventual_with_reset(first_series, last_series, case_series):
    row_positions = pd.Series(range(len(case_series)), index=case_series.index, dtype=float)

    last_a_position = row_positions.where(first_series).groupby(case_series).ffill()
    previous_b_position = row_positions.where(last_series).groupby(case_series).ffill().groupby(case_series).shift(+1)

    return last_series & (last_a_position > previous_b_position.fillna(float("-inf")))

def _condition_to_query_expression(condition):
    if isinstance(condition, DataObjectState):
        return condition.name
    return str(condition)


def _check_column_existence(dataframe, activity_column, transition_column):
    return (activity_column and transition_column in dataframe.columns)


def _check_end_condition(condition):
    return condition == RuntimeState.END


def _check_state(condition):
    if(condition == RuntimeState.START):
        return "'Assigned'"
    elif(condition == RuntimeState.END):
        return "'Completed'"
    else:
        return condition
