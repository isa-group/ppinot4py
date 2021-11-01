from ppinot4py.computers.conditions_computer import condition_computer
from ppinot4py.model import (
    CountMeasure, 
    DataMeasure, 
    TimeMeasure, 
    AggregatedMeasure,
    DerivedMeasure,
    BusinessDuration  
)
from ppinot4py.model.measures import _MeasureDefinition

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_timedelta64_dtype as is_timedelta
from pandas.api.types import is_numeric_dtype, is_bool_dtype
import numpy as np
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


def measure_computer(measure, dataframe, log_configuration: LogConfiguration = None, time_grouper = None):
    """ General computer.
    
    Args:    
        - measure: Measure, it will call different computers depending on the type.
        - dataframe: Base dataframe we want to use.
        - log_configuration (optional): LogConfiguration that specifies the names of special columns of the log
        - time_grouper (optional): Time grouper (https://pandas.pydata.org/docs/user_guide/timeseries.html)
            without the key. If the measure is aggregated, it groups the result by instance end time
            
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
            computer = time_compute(dataframe,measure, log_configuration)
        elif(type(measure) == AggregatedMeasure):
            computer = aggregated_compute(dataframe, measure, log_configuration, time_grouper)
        elif(type(measure) == DerivedMeasure):
            computer = derived_compute(dataframe, measure, log_configuration, time_grouper)
        return computer
    except ValueError as err:
        raise ValueError("ERROR: A value in the measure wasn't correctly defined") from err


def count_compute(dataframe, measure, log_configuration):
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

def time_compute(dataframe, measure, log_configuration):
    operation = measure.single_instance_agg_function
    precondition = measure.precondition
    time_measure_type = measure.time_measure_type
    from_condition = measure.from_condition
    to_condition = measure.to_condition
    is_first = measure.first_to
    
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
        final_result = _linear_time_compute(dataframe_to_work, A_condition, B_condition, is_first, 'id', 't', measure)   
    elif(time_measure_type == 'CYCLIC'):
        final_result = _cyclic_time_compute(dataframe_to_work, A_condition, B_condition, operation, 'id', 't', measure)
       
    return final_result.reindex(dataframe[id_case].unique())

def _linear_time_compute(dataframeToWork, from_condition, to_condition, is_first, id_case, time_column, measure):
    filtered_dataframe_A = dataframeToWork[from_condition]
    from_values = filtered_dataframe_A.groupby(id_case)[time_column].first()
    
    if(is_first):
        to_values = _first_to_after_from(from_condition, to_condition, dataframeToWork[id_case], dataframeToWork[time_column])
    else:
        finalDataframeB = dataframeToWork[to_condition]        
        to_values = finalDataframeB.groupby(id_case)[time_column].last()
    
    final_result = _linear_calculation(from_values, to_values, measure)

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

def _linear_calculation(from_values, to_values, measure):

    if(measure.business_duration is not None):
        result_serie = business_duration_calculation(measure, from_values, to_values)
        return result_serie    
    else:
        finalResult = to_values - from_values   
        finalResult[finalResult < pd.Timedelta(0)] = pd.Timedelta('nan')
        return finalResult    
#    finalResultWithNaN = finalResultAlmost.groupby(id_case).max()
#    finalResultWithNegatives = finalResultWithNaN.dropna()
#    finalResultDataframe = finalResultWithNegatives.to_frame('data')
        
#    finalResult = finalResultDataframe[finalResultDataframe['data'] > pd.Timedelta(0)]
#    finalResult = finalResultAlmost[finalResultAlmost > pd.Timedelta(0)]
    
    

def _cyclic_time_compute(dataframeToWork, A_condition, B_condition, operation, id_case, time_column, measure):        

    diff = _compute_cyclic_diff(A_condition, B_condition, dataframeToWork[id_case], dataframeToWork[time_column], measure)

    # We remove NaNs so that they do not get evaluated as 0 with sum
    diff = diff.dropna()

    # This is necessary because sum() is not allowed in TimeDeltas
    if(measure.business_duration is None):
        diff = diff.dt.total_seconds()
        conversion = lambda x: datetime.timedelta(seconds = x)
    else:
        conversion = measure.business_duration.conversion()

    grouped_diff = diff.groupby(dataframeToWork[id_case])
    result = _apply_aggregation(operation, grouped_diff)

    return result.apply(conversion)  
    

def _apply_aggregation(operation, grouped_df):

    if not isinstance(operation, str):
        raise ValueError('Aggregation function must be a string: sum, min, max, avg or groupby')

    if(operation.upper() == 'SUM'):
        result = grouped_df.sum(min_count=1)

    elif(operation.upper() == "MIN"):
        result = grouped_df.min()

    elif(operation.upper() == "MAX"):
        result = grouped_df.max()

    elif(operation.upper() == "AVG"):
        result = grouped_df.mean()

    elif(operation.upper() == "GROUPBY"):
        result = grouped_df

    else:
        raise ValueError(f'Aggregation operation not valid {operation}. Should be: sum, min, max, avg or groupby')

    return result


def _compute_cyclic_diff(from_condition, to_condition, id_case, timestamps, measure):
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

    if(measure.business_duration is not None):
        return business_duration_calculation(measure, df.loc[pair,'t'], df_shifted.loc[pair, 't'])
    else:
        return df_shifted.loc[pair, 't'] - df.loc[pair,'t']

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

def aggregated_compute(dataframe, measure, log_configuration, time_grouper = None):
    base_measure = measure.base_measure
    filter_to_apply = measure.filter_to_apply
    operation = measure.single_instance_agg_function
    data_grouper = measure.grouper
    id_case = log_configuration.id_case
    time_column = log_configuration.time_column

    is_time = False

    base_values = measure_computer(base_measure, dataframe, log_configuration)
    
    case_end = dataframe.groupby(id_case)[time_column].last()
    
    if((filter_to_apply is not None) and filter_to_apply != ""):
        filter_condition = measure_computer(filter_to_apply, dataframe, log_configuration)
        # We assume the filtered_condition is fine. Maybe we could do
        # some sanity checking here.
        base_values = base_values[filter_condition]
        case_end = case_end[filter_condition]

    # Case end could also be configurable
    internal_df = pd.DataFrame({'data':base_values, 'case_end':case_end})

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
            
                internal_df[gr_key] = measure_computer(gr, dataframe, log_configuration)
                groupers.append(gr_key)
            else:
                groupers.append(gr)

    if time_grouper is not None:
        internal_time_grouper = copy(time_grouper)
        internal_time_grouper.key = 'case_end'
        groupers.append(internal_time_grouper)

    if len(groupers) > 0:
        result_grouped = internal_df.groupby(groupers)
    else:
        result_grouped = internal_df

    final_result = _apply_aggregation(operation, result_grouped)

    if(operation.upper() == "GROUPBY"):
        is_time = False

    if len(final_result) > 1:
        if is_time == True:
            final_result = final_result['data'].apply(lambda x: datetime.timedelta(seconds = x) if not np.isnan(x) else np.nan)
        else:
            final_result = final_result['data']
        #final_result = final_result.drop('case_end', axis=0, errors='ignore')
    else:
        if is_time == True:
            final_result = datetime.timedelta(seconds = final_result['data']) if not np.isnan(final_result['data']) else np.nan
        else:
            final_result = final_result['data']

    return final_result

def derived_compute(dataframe, measure, log_configuration, time_grouper=None):
    
    function = measure.function_expression
    measure_map = measure.measure_map
    data_frame_computer = pd.DataFrame()
    id_case = log_configuration.id_case
    time_column = log_configuration.time_column
    istime = False
    
    for key in measure_map: 
        if isinstance(measure_map[key], _MeasureDefinition):
            data_frame_computer[key] = measure_computer(measure_map[key], dataframe, log_configuration, time_grouper)        
        else:
            data_frame_computer[key] = measure_map[key]

        if(is_timedelta(data_frame_computer[key].dtype)):
            data_frame_computer[key] = data_frame_computer[key].dt.total_seconds()
            #data_frame_computer[key] = data_frame_computer[key].fillna(0).astype(float)

            # If there is one time, we assume everything is time
            istime = True
  
    evaluated_dataframe = data_frame_computer.eval(function)
    #evaluated_dataframe_noInfinites = evaluated_dataframe.replace([np.inf, -np.inf], 0)
    #final_result = evaluated_dataframe_noInfinites.fillna(0).astype(float)

    final_result = evaluated_dataframe
    
    if istime and is_numeric_dtype(final_result.dtype) and not is_bool_dtype(final_result.dtype):
        final_result = final_result.apply(lambda x: datetime.timedelta(seconds = x) if not np.isnan(x) else np.nan)
    
    return final_result


def business_duration_calculation(measure, from_values, to_values):

    open_time= measure.business_duration.business_start
    close_time= measure.business_duration.business_end
    holiday_list = measure.business_duration.holiday_list
    unit_hour= measure.business_duration.unit_hour
    weekend_list = measure.business_duration.weekend_list

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
                                repeat(unit_hour))),
                                index=from_values.index)

    return business_diff

