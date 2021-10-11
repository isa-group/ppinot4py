from ppinot4py.model import *
from ppinot4py.computers import *
from datetime import time
import holidays as pyholidays
import datetime
import pandas as pd
import pytest
import numpy as np

@pytest.fixture
def log_for_time():
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    IdCase3 = '3-364285768'
        
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2011, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2013, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2014, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2015, 6, 6, 16, 44, 7)
    
        
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase2, IdCase2, IdCase3, IdCase3], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment' ]}

    dataframeLinear = pd.DataFrame(data)

    return dataframeLinear

@pytest.fixture
def log_config():

    return LogConfiguration()


def test_aggregated_compute_time_grouped(log_for_time, log_config):

    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)

    aggregatedMeasure = AggregatedMeasure(
        base_measure=timeMeasureLinearA, 
        single_instance_agg_function='SUM')
        
    timeResult1 = datetime.timedelta(days=365, minutes=46, seconds=6) 
    
    var = measure_computer(aggregatedMeasure, log_for_time, log_config, time_grouper=pd.Grouper(freq='1Y'))
    
    assert var.size == 5
    assert var.iloc[0] == timeResult1
    assert pd.isnull(var.iloc[1])
    assert var.iloc[2] == datetime.timedelta(days=365)

def test_aggregated_compute_time_no_group(log_for_time, log_config):

    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)

    aggregatedMeasure = AggregatedMeasure(
        base_measure=timeMeasureLinearA, 
        single_instance_agg_function='SUM')

    var = measure_computer(aggregatedMeasure, log_for_time, log_config)

    assert var == datetime.timedelta(days=1126, minutes=46, seconds =6)

def test_derived_compute_time_condition(log_for_time, log_config):
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)
    
    derivedMeasure = DerivedMeasure("time < days366", 
        {"time": timeMeasureLinearA,
         "days366": pd.Timedelta(days=366)})

    var = measure_computer(derivedMeasure, log_for_time, log_config)

    assert var.size == 3
    assert var.iloc[0]
    assert var.iloc[1]
    assert not var.iloc[2]

def test_derived_aggregated_time(log_for_time, log_config):
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)

    aggregatedMeasure = AggregatedMeasure(
        base_measure=timeMeasureLinearA, 
        single_instance_agg_function='SUM')

    derivedMeasure = DerivedMeasure("time < days366", 
        {"time": aggregatedMeasure,
         "days366": pd.Timedelta(days=366)})

    var = measure_computer(derivedMeasure, log_for_time, log_config, time_grouper=pd.Grouper(freq='1Y'))

    assert var.size == 5
    assert np.all(var == [True, False, True, False, False])


def test_count_compute(log_config):
    
    countMeasure = CountMeasure('`lifecycle:transition` == "In Progress"')

    IdCase1 = '1-364285768'
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(countMeasure, dataframeLinear, log_config).iloc[0]

    assert var == 2
    
def test_count_compute_not_appear(log_config):
    
    countMeasure = CountMeasure('`lifecycle:transition` == "Closed"')
    
    IdCase1 = '1-364285768'
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(countMeasure, dataframeLinear, log_config).iloc[0]

    assert var == 0
    
def test_count_compute_instances(log_config):
    
    countMeasure = CountMeasure('`lifecycle:transition` == "In Progress"')
    
    IdCase1 = '1-364285768'
    idCase2 = '2-364285768'
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, idCase2, idCase2, idCase2], 
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(countMeasure, dataframeLinear, log_config)

    assert var.iloc[0] == 2
    assert var.iloc[1] == 1

def test_data_computer(log_config):
    
    dataMeasure = DataMeasure(
        data_content_selection="lifecycle:transition", 
        first=False)

    IdCase1 = '1-364285768'
    
    data = {'case:concept:name': [IdCase1, IdCase1, IdCase1], 
            'concept:name': ['Queued', 'Queued', 'Not queued'],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','Completed']}
            
    dataframeLinear = pd.DataFrame(data)
    result = measure_computer(dataMeasure, dataframeLinear, log_config)

    assert result.iloc[0] == 'Completed'

def test_data_computer_precondition(log_config):
    
    dataMeasure = DataMeasure(
        precondition="`concept:name` == 'Queued'", 
        data_content_selection="lifecycle:transition", 
        first=False)

    IdCase1 = '1-364285768'
    
    data = {'case:concept:name': [IdCase1, IdCase1, IdCase1], 
            'concept:name': ['Queued', 'Queued', 'Not queued'],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    result = measure_computer(dataMeasure, dataframeLinear, log_config)
    
    assert result.iloc[0] == 'In Progress'

def test_time_linear_instances(log_config):
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    
    IdCase1 = '1-364285768'
    
    timeResult = datetime.timedelta(minutes=46, seconds=6) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult

def test_time_linear_instances_with_several_from(log_config):
        
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=False)
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)

    timeResult = datetime.timedelta(minutes=46, seconds=6) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress' ]}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult
    
def test_time_linear_instances_WithSeveralToAndFirstTo(log_config):
        
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True)
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)

    timeResult = datetime.timedelta(minutes=46, seconds=6) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress' ]}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult
    

def test_time_linear_instances_WithSeveralToAndNotFirstTo(log_config):
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=False)
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)

    timeResult = datetime.timedelta(days= 797, hours=23, minutes=44, seconds=25) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment' ]}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult
    
    
def test_time_linear_instances_WithSeveralFromAndToAndFirstTo2(log_config):
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=False)
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days= 797, hours=23, minutes=44, seconds=25) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult
    
def test_time_cyclic_SumInstances(log_config):
    
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='SUM',
        time_measure_type='CYCLIC')
    
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days= 732, minutes=46, seconds=6) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0]
    
    assert var == timeResult
    
def test_time_cyclic_MaxInstances(log_config):
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='MAX',
        time_measure_type='CYCLIC')
    
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days= 731) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0] 
    
    assert var == timeResult
    
def test_time_cyclic_AvgInstances(log_config):
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='AVG',
        time_measure_type='CYCLIC')
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days=191, hours=12, minutes=11, seconds=31.5) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0]
    
    assert var == timeResult

def test_derived_instances(log_config):
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Awaiting Assignment"',
        first_to=True)
    
    timeMeasureLinearB = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Closed"',
        first_to=True)
    
    timeMeasureLinearC = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"',
        to_condition='`lifecycle:transition` == "Pending"',
        first_to=True)
    
    measure_dictionary = {'ProgressTime': timeMeasureLinearA, 'ClosedTime': timeMeasureLinearB, 'PendingTime': timeMeasureLinearC}
    derived_measure = DerivedMeasure(
        function_expression='(ProgressTime + ClosedTime) + PendingTime', 
        measure_map=measure_dictionary)
    
    IdCase1 = '1-364285768'
    
    time_result = datetime.timedelta(days= 3360, minutes=14, seconds=56) 
        
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2011, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2013, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2014, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2015, 6, 6, 16, 44, 7)
        
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Closed', 'In Progress', 'Pending']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(derived_measure, dataframeLinear, log_config)
    
    assert var[0] == time_result


def test_time_linear_instances_businessDuration_from_7_to_17(log_config):

    business = BusinessDuration(
        business_start = time(7,0,0),
        business_end = time(17,0,0),
        weekend_list = [5,6],
        holiday_list = pyholidays.ES(prov ='AN'),
        unit_hour = 'sec'
    )
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=True,
        business_duration = business)
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    
    IdCase1 = '1-364285768'
    
    timeResult = 18
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
            
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult

def test_time_linear_instances_businessDuration_from_7_to_17_severalFrom(log_config):

    business = BusinessDuration(
        business_start = time(7,0,0),
        business_end = time(17,0,0),
        weekend_list = [5,6],
        holiday_list = pyholidays.ES(prov ='AN'),
        unit_hour = 'sec'
    )
    
    timeMeasureLinearA = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        first_to=False,
        business_duration = business)
    
    IdCase1 = '1-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
    time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)

    timeResult = 18 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress' ]}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureLinearA, dataframeLinear, log_config).iloc[0]
    assert var == timeResult

def test_time_cyclic_SumInstances_businessDuration(log_config):
    
    business = BusinessDuration(
        business_start = time(7,0,0),
        business_end = time(17,0,0),
        weekend_list = [5,6],
        holiday_list = pyholidays.ES(prov ='AN'),
        unit_hour = 'hour'
    )
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='SUM',
        time_measure_type='CYCLIC',
        business_duration = business)
    
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days=209, hours=14, seconds=18) 
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0]
    
    assert var == timeResult

def test_time_cyclic_MaxInstances_businessDuration(log_config):

    business = BusinessDuration(
        business_start = time(7,0,0),
        business_end = time(17,0,0),
        weekend_list = [5,6],
        holiday_list = pyholidays.ES(prov ='AN'),
        unit_hour = 'hour'
    )
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='MAX',
        time_measure_type='CYCLIC',
        business_duration = business)
    
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days=209, hours=4, minutes=15, seconds=53)  
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0] 
    
    assert var == timeResult

def test_time_cyclic_AVGInstances_businessDuration(log_config):

    business = BusinessDuration(
        business_start = time(7,0,0),
        business_end = time(17,0,0),
        weekend_list = [5,6],
        holiday_list = pyholidays.ES(prov ='AN'),
        unit_hour = 'hour'
    )
    
    timeMeasureCyclic = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='AVG',
        time_measure_type='CYCLIC',
        business_duration = business)
    
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    timeResult = datetime.timedelta(days=69, hours=20, minutes=40, seconds=6)   
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)
    var = measure_computer(timeMeasureCyclic, dataframeLinear, log_config).iloc[0] 
    
    assert var == timeResult


def test_data_computer_precondition_predefined_log_values(log_config):

    precondition = TimeInstantCondition("'Awaiting Assignment'", AppliesTo.ACTIVITY, "'Queued'")

    dataMeasure = DataMeasure(
            data_content_selection="lifecycle:transition", 
            precondition = precondition,
            first=False)

    IdCase1 = '1-364285768'
    IdCase2 = '1-364285769'

    data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','Completed', 'Awaiting Assignment','Completed']}
            
    dataframeLinear = pd.DataFrame(data)

    result = measure_computer(dataMeasure, dataframeLinear, log_config)

    assert result.iloc[0] == 'Awaiting Assignment'

def test_data_computer_precondition_non_predefined_log_values(log_config):

    precondition = TimeInstantCondition("'Awaiting Assignment'", AppliesTo.ACTIVITY, "'Queued'")

    log = LogConfiguration(
        id_case = 'case:concept:name', 
        time_column = 'time:timestamp', 
        transition_column = 'lifecycle:transition:transition', 
        activity_column = 'concept:name:non:predefined')

    dataMeasure = DataMeasure(
            data_content_selection="lifecycle:transition:transition", 
            precondition = precondition,
            first=False)

    IdCase1 = '1-364285768'
    IdCase2 = '1-364285769'

    data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'concept:name:non:predefined': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
            'lifecycle:transition:transition': ['In Progress', 'Awaiting Assignment','Completed', 'Awaiting Assignment','Completed']}
            
    dataframeLinear = pd.DataFrame(data)

    result = measure_computer(dataMeasure, dataframeLinear, log)

    assert result.iloc[0] == 'Awaiting Assignment'