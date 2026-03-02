from ppinot4py.computers.conditions_computer import condition_computer
from ppinot4py.model import (
    CountMeasure, 
    DataMeasure, 
    TimeMeasure, 
    AggregatedMeasure,
    DerivedMeasure
)
from ppinot4py.model.measures import _MeasureDefinition, RollingWindow

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_timedelta64_dtype as is_timedelta
from pandas.api.types import is_numeric_dtype, is_bool_dtype
import numpy as np
import re
from copy import copy
import datetime

from business_duration import businessDuration
from itertools import repeat

class LogConfiguration():
    """
    Class that specifies the configuration (in terms of the names of the attributes) of the log that is 
    being used in the computation.
    
    Args:
        - id_case (optional): Id column of your dataframe (By default is 'case:concept:name')
        - time_column (optional): Timestamp column of your dataframe (By default is 'time:timestamp').
        - transition_column (optional): Transition column of the dataframe (By default 'lifecycle:transition') 
        - activity_column (optional): Activity column of the dataframe (By default 'concept:name')

    """    
    def __init__(self, id_case = 'case:concept:name', time_column = 'time:timestamp', transition_column = 'lifecycle:transition', activity_column = 'concept:name'):

        self.id_case = id_case
        self.time_column = time_column
        self.transition_column = transition_column
        self.activity_column = activity_column


def measure_computer(measure: _MeasureDefinition, dataframe: pd.DataFrame, log_configuration: LogConfiguration | None = None, time_grouper = None, business_duration = None):
    """ General computer.
    
    Args:    
        - measure: The measure definition that will be computed
        - dataframe: Dataframe that contains the event log to compute the measure
        - log_configuration (optional): LogConfiguration that specifies the names of special columns of the log
        - time_grouper (optional): Time grouper (https://pandas.pydata.org/docs/user_guide/timeseries.html)
            without the key. If the measure is aggregated, it groups the result by instance end time
        - business_duration (optional): Default business duration applied to TimeMeasures that do not
            define their own business_duration.
            
    Returns:
        - Series: Series with pairs of case ID - Data

    """

    if log_configuration is None:
        log_configuration = LogConfiguration()

    try:
        # Need to change ":" for "_" because ".query" put nervous with ":"
        # dataframe.columns = [column.replace(":", "_") for column in dataframe.columns]
        # Evaluation wich kind of measure is
        if(type(measure) == CountMeasure):
            computer = count_compute(dataframe,measure, log_configuration)
        elif(type(measure) == DataMeasure):
            computer = data_compute(dataframe,measure, log_configuration)
        elif(type(measure) == TimeMeasure):
            computer = time_compute(dataframe, measure, log_configuration, business_duration)
        elif(type(measure) == AggregatedMeasure):
            computer = aggregated_compute(dataframe, measure, log_configuration, time_grouper, business_duration)
        elif(type(measure) == DerivedMeasure):
            computer = derived_compute(dataframe, measure, log_configuration, time_grouper, business_duration)
        else:
            raise ValueError("ERROR: Measure not valid. It should be count, data, time, aggregated and derived")
        return computer
    except ValueError as err:
        raise ValueError("ERROR: A value in the measure wasn't correctly defined") from err


def count_compute(dataframe: pd.DataFrame, measure: CountMeasure, log_configuration: LogConfiguration):
    id_case = log_configuration.id_case
    precondition = (measure.when)
    filtered_series = condition_computer(dataframe, id_case, precondition, log_configuration.activity_column, log_configuration.transition_column)
    result = filtered_series.groupby(dataframe[id_case]).sum()

    return result

def data_compute(dataframe, measure, log_configuration):
    precondition = (measure.precondition)
    id_case = log_configuration.id_case
    if precondition is not None:
        filtered_series = condition_computer(dataframe, id_case, precondition, log_configuration.activity_column, log_configuration.transition_column)
        final_dataframe = dataframe[filtered_series]
    else:
        final_dataframe = dataframe
    
   
    if measure.first:
        result = final_dataframe.groupby(id_case)[measure.data_content_selection].first()
    else:
        result = final_dataframe.groupby(id_case)[measure.data_content_selection].last()

    return result

def time_compute(dataframe, measure, log_configuration, business_duration=None):
    operation = measure.single_instance_agg_function
    precondition = measure.precondition
    time_measure_type = measure.time_measure_type.upper() if isinstance(measure.time_measure_type, str) else measure.time_measure_type
    from_condition = measure.from_condition
    to_condition = measure.to_condition
    is_first = measure.first_to
    time_unit = measure.time_unit
    effective_business_duration = measure.business_duration if measure.business_duration is not None else business_duration
    
    id_case = log_configuration.id_case
    transition_column = log_configuration.transition_column
    activity_column = log_configuration.activity_column
    time_column = log_configuration.time_column
    
    id_case = log_configuration.id_case
    transition_column = log_configuration.transition_column
    activity_column = log_configuration.activity_column
    time_column = log_configuration.time_column
    

    # Initial precondition
    if(precondition is not None):
        filtered_dataframe = dataframe.query(precondition)
    else:
        filtered_dataframe = dataframe
   
    A_condition = condition_computer(filtered_dataframe, id_case, from_condition, activity_column, transition_column)
    B_condition = condition_computer(filtered_dataframe, id_case, to_condition, activity_column, transition_column)

    removed_timezones = pd.to_datetime(filtered_dataframe[time_column], utc=True)
    dataframe_to_work = pd.DataFrame({'id':filtered_dataframe[id_case], 't': removed_timezones})

    if(time_measure_type == 'LINEAR'): 
        final_result = _linear_time_compute(dataframe_to_work, A_condition, B_condition, is_first, 'id', 't', effective_business_duration)
    elif(time_measure_type == 'CYCLIC'):
        final_result = _cyclic_time_compute(dataframe_to_work, A_condition, B_condition, operation, 'id', 't', effective_business_duration)
    else:
        raise ValueError("ERROR: Time measure type is not valid. It should be LINEAR or CYCLIC")

    result_reindex = final_result.reindex(dataframe[id_case].unique(), fill_value=pd.NaT)

    if(time_unit != None):
        return result_reindex/ np.timedelta64(1, time_unit.value)
    else:
        return result_reindex

def _linear_time_compute(df, from_condition, to_condition, is_first, id_case, time_column, business_duration=None):
    filtered_dataframe_A = df[from_condition]
    from_values = filtered_dataframe_A.groupby(id_case)[time_column].first()
    
    if(is_first):
        to_values = _first_to_after_from(from_condition, to_condition, df[id_case], df[time_column])
    else:
        finalDataframeB = df[to_condition]        
        to_values = finalDataframeB.groupby(id_case)[time_column].last()
    
    final_result = _linear_calculation(from_values, to_values, business_duration)

    return final_result

def _first_to_after_from(from_condition, to_condition, id_values, ts_values):
    df = pd.DataFrame({'id':id_values, 't': ts_values})
    df.loc[from_condition, 'c']='A'
    df.loc[to_condition, 'c']='B'

    # Keep only the events with from, to conditions
    df = df.dropna()

    # Choose only those 'to' after a 'from'
    df['prev'] = df.groupby('id')['c'].shift(+1).fillna('B')
    df = df[((df['c']=='B') & (df['prev']=='A'))]

    # Returns the first of them
    return df.groupby('id')['t'].first()

def _linear_calculation(from_values, to_values, business_duration=None):

    if business_duration is not None:
        result_serie = business_duration_calculation(business_duration, from_values, to_values)
    else:
        result_serie = to_values - from_values   
    
    result_serie[result_serie < pd.Timedelta(0)] = pd.Timedelta('nan')    
    return result_serie    
    

def _cyclic_time_compute(dataframeToWork, A_condition, B_condition, operation, id_case, time_column, business_duration=None):

    diff = _compute_cyclic_diff(A_condition, B_condition, dataframeToWork[id_case], dataframeToWork[time_column], business_duration)

    # We remove NaNs so that they do not get evaluated as 0 with sum
    diff = diff.dropna()

    grouped_diff = diff.groupby(dataframeToWork[id_case])
    result = _apply_aggregation(operation, grouped_diff)

    return result
    

def _apply_aggregation(operation, grouped_df):
    if not isinstance(operation, str):
        raise ValueError('Aggregation function must be a string: sum, min, max, avg, median, pXX or groupby')

    normalized_operation = operation.strip().upper()
    percentile = _parse_percentile_operation(normalized_operation)

    if normalized_operation == 'SUM':
        is_rolling = hasattr(grouped_df, "window") and hasattr(grouped_df, "sum") and hasattr(grouped_df, "count")
        if is_rolling:
            result = grouped_df.sum()
        else:
            result = grouped_df.sum(min_count=1)

    elif normalized_operation == "MIN":
        result = grouped_df.min()

    elif normalized_operation == "MAX":
        result = grouped_df.max()

    elif normalized_operation == "AVG":
        result = grouped_df.mean()

    elif normalized_operation == "MEDIAN":
        result = grouped_df.median()

    elif percentile is not None:
        result = grouped_df.quantile(percentile)

    elif normalized_operation == "GROUPBY":
        result = grouped_df

    else:
        raise ValueError(f'Aggregation operation not valid {operation}. Should be: sum, min, max, avg, median, pXX or groupby')

    return result

def _parse_percentile_operation(operation):
    match = re.fullmatch(r"P(\d{1,3})", operation)
    if not match:
        return None

    percentile = int(match.group(1))
    if percentile < 1 or percentile > 99:
        raise ValueError(f'Aggregation percentile {operation} not valid. XX must be in range 1..99')

    return percentile / 100


def _compute_cyclic_diff(from_condition, to_condition, id_case, timestamps, business_duration=None):
    df = pd.DataFrame({'id':id_case, 't': timestamps})
    df.loc[from_condition, 'c']='A'
    df.loc[to_condition, 'c']='B'

    # Keep only the events with from, to conditions
    df = df.dropna()

    # Removes several As (AAA) or several Bs (BBB) by keeping only
    # the first event of each series, i.e., those whose previous 
    # event was different. So, now what we have is a sequence of
    # ABABA or similar
    df['prev'] = df.groupby(id_case)['c'].shift(+1).fillna('B')
    df = df[(((df['c']=='A') & (df['prev']=='B')) | ((df['c']=='B') & (df['prev']=='A')))]

    # Compute the difference for each AB pair
    df_shifted = df.groupby(id_case).shift(-1)

    pair = (df['c']=='A')

    return _linear_calculation(df.loc[pair,'t'], df_shifted.loc[pair, 't'], business_duration)

    # if(measure.business_duration is not None):
    #     return business_duration_calculation(measure, df.loc[pair,'t'], df_shifted.loc[pair, 't'])
    # else:
    #     return df_shifted.loc[pair, 't'] - df.loc[pair,'t']

# This method is an alternative implementation to _compute_cyclic_diff
# and might be removed in the future
def _compute_cyclic_diff_alt(from_condition, to_condition, id_case, timestamps):
    combination = _combine(from_condition, to_condition)
    first_change_index = _first_change_index(_detect_change_in_serie(combination, id_case), id_case)
    
    df = pd.DataFrame({'id':id_case, 't': timestamps, 'c': combination})
    df = df.loc[first_change_index]
    df_shifted = df.groupby(id_case).shift(-1)
    pair = (df['c']=='A')
    return df_shifted.loc[pair, 't'] - df.loc[pair,'t']

def _combine(A, B):
    combination = A.copy()
    combination.loc[A] = 'A'
    combination.loc[B] = 'B'
    return combination[(A | B)]

def _detect_change_in_serie(s, id_case):
    return (s != s.groupby(id_case).shift(+1))

def _first_change_index(s, id_case):
    cum_change = s.groupby(id_case).cumsum()
    return s.index.to_series().groupby([id_case, cum_change]).first()

def aggregated_compute(dataframe, measure, log_configuration, time_grouper = None, business_duration = None):
    base_measure = measure.base_measure
    filter_to_apply = measure.filter_to_apply
    operation = measure.single_instance_agg_function
    data_grouper = measure.grouper
    id_case = log_configuration.id_case
    time_column = log_configuration.time_column

    if ((data_grouper is not None) and (len(data_grouper) > 0)) and (isinstance(time_grouper, RollingWindow)):
        raise ValueError('A rolling time_grouper cannot be used with an AggregatedMeasure with group by')

    is_time = False

    base_values = measure_computer(base_measure, dataframe, log_configuration, business_duration=business_duration)
    
    case_end = dataframe.groupby(id_case)[time_column].last()

    
    if((filter_to_apply is not None) and filter_to_apply != ""):
        filter_condition = measure_computer(filter_to_apply, dataframe, log_configuration, business_duration=business_duration)
        # We assume the filtered_condition is fine. Maybe we could do
        # some sanity checking here.
        base_values = base_values[filter_condition]
        case_end = case_end[filter_condition]

    # Case end could also be configurable
    internal_df = pd.DataFrame({'data':base_values, 'case_end':case_end}).sort_values(by='case_end')

    if not is_datetime(internal_df['case_end']):
        internal_df['case_end'] = pd.to_datetime(internal_df['case_end'], utc=True)
      
    if is_timedelta(internal_df['data'].dtype):
        internal_df['data'] = internal_df['data'].dt.total_seconds()
        #internal_df['data_seconds'] = internal_df['data_seconds'].fillna(0).astype(float)
        is_time = True

    groupers = []
    if (data_grouper is not None):
        if not isinstance(data_grouper, list):
            data_grouper = [data_grouper]
            
        for gr in data_grouper:
            if not isinstance(gr, str):
                gr_key = gr.id
                if gr_key is None:
                    gr_key = 'group' + str(len(groupers))
            
                internal_df[gr_key] = measure_computer(gr, dataframe, log_configuration, business_duration=business_duration)
                groupers.append(gr_key)
            else:
                groupers.append(gr)

    if time_grouper is not None:
        temp_grouper = None
        if isinstance(time_grouper, str):
            temp_grouper = pd.Grouper(freq=time_grouper)
        elif isinstance(time_grouper, RollingWindow):
            if not time_grouper.apply_to_cases:
                offset = pd.tseries.frequencies.to_offset(time_grouper.window)
                temp_grouper = pd.Grouper(freq=offset.base)
        else:
            temp_grouper = time_grouper

        if temp_grouper is not None:
            internal_time_grouper = copy(temp_grouper)
            internal_time_grouper.key = 'case_end'
            groupers.append(internal_time_grouper)
    
    if len(groupers) > 0:
        result_grouped = internal_df.groupby(groupers)
    else:
        result_grouped = internal_df

    normalized_operation = operation.strip().upper() if isinstance(operation, str) else operation

    if not isinstance(time_grouper, RollingWindow):
        final_result = _apply_aggregation(operation, result_grouped["data"])
    else:
        roll_window = time_grouper.window
        roll_min_period = time_grouper.min_period
        roll_closed = time_grouper.closed
        if time_grouper.apply_to_cases:
            rolling_result = result_grouped.rolling(roll_window, min_periods=roll_min_period, on='case_end', closed=roll_closed)
            final_result = _apply_aggregation(operation, rolling_result)
        else:
            if normalized_operation == 'AVG':
                rolling_sum = result_grouped.sum().rolling(roll_window, min_periods=roll_min_period, closed=roll_closed).sum()
                rolling_count = result_grouped.count().rolling(roll_window, min_periods=roll_min_period, closed=roll_closed).sum()
                final_result = rolling_sum / rolling_count
            elif normalized_operation == 'MEDIAN' or (
                isinstance(normalized_operation, str) and _parse_percentile_operation(normalized_operation) is not None
            ):
                raise ValueError('MEDIAN and PXX are not supported with RollingWindow when apply_to_cases is False')
            else:   
                agg_result = _apply_aggregation(operation, result_grouped)
                rolling_result = agg_result.rolling(roll_window, min_periods=roll_min_period, closed=roll_closed)
                final_result = _apply_aggregation(operation, rolling_result)

    if(normalized_operation == "GROUPBY"):
        is_time = False

    if isinstance(final_result, pd.DataFrame):
        final_result = final_result['data']

    if isinstance(final_result, pd.Series):   
        if is_time == True:
            final_result = final_result.apply(lambda x: datetime.timedelta(seconds = x) if not np.isnan(x) else np.nan)
            #final_result = final_result.drop('case_end', axis=0, errors='ignore')
    else:
        if is_time == True:
            final_result = datetime.timedelta(seconds = final_result) if not np.isnan(final_result) else np.nan


    return final_result

def derived_compute(dataframe, measure, log_configuration, time_grouper=None, business_duration=None):
    function = measure.function_expression
    measure_map = measure.measure_map
    resolved_values = {}
    reference_index = None
    istime = False

    for key in measure_map:
        if isinstance(measure_map[key], _MeasureDefinition):
            value = measure_computer(measure_map[key], dataframe, log_configuration, time_grouper, business_duration)
        else:
            value = measure_map[key]

        if isinstance(value, pd.Series) and reference_index is None:
            reference_index = value.index

        resolved_values[key] = value

    if reference_index is None:
        scalar_context = {}
        for key, value in resolved_values.items():
            if isinstance(value, pd.Timedelta):
                scalar_context[key] = value.total_seconds()
                istime = True
            else:
                scalar_context[key] = value

        final_result = pd.eval(function, local_dict=scalar_context)
        if istime and pd.notna(final_result):
            return pd.Timedelta(seconds=final_result)
        return final_result

    data_frame_computer = pd.DataFrame(index=reference_index)
    for key, value in resolved_values.items():
        if isinstance(value, pd.Series):
            column = value.reindex(reference_index)
        else:
            column = pd.Series(value, index=reference_index)

        if is_timedelta(column.dtype):
            column = column.dt.total_seconds()
            istime = True
        elif isinstance(value, pd.Timedelta):
            column = pd.Series(value.total_seconds(), index=reference_index)
            istime = True

        data_frame_computer[key] = column

    final_result = data_frame_computer.eval(function)

    if istime and is_numeric_dtype(final_result.dtype) and not is_bool_dtype(final_result.dtype):
        final_result = final_result.apply(lambda x: pd.Timedelta(seconds=x) if pd.notna(x) else pd.NaT)

    return final_result


def business_duration_calculation(business_duration, from_values, to_values):

    open_time= business_duration.business_start
    close_time= business_duration.business_end
    holiday_list = business_duration.holiday_list
    weekend_list = business_duration.weekend_list

    from_values.name = 'start'
    to_values.name = 'end'
    dataf = pd.concat([from_values, to_values], axis=1)
    business_diff = pd.Series(data=list(map(businessDuration,
                                dataf['start'],
                                dataf['end'],
                                repeat(open_time),
                                repeat(close_time),
                                repeat(weekend_list),
                                repeat(holiday_list),
                                repeat('sec'))),
                                index=from_values.index)

    result = pd.Series(pd.NaT, index=business_diff.index, dtype="timedelta64[ns]")
    valid = business_diff.notna()
    result.loc[valid] = pd.to_timedelta(business_diff.loc[valid], unit="sec")

    return result
