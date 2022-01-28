from ppinot4py.model import *
from ppinot4py.computers import *
from datetime import time
import holidays as pyholidays
import datetime
import pandas as pd
import pytest
import numpy as np
import math

from pm4py.objects.conversion.log import converter as log_converter
from ppinot4py import model
from datetime import time
from itertools import repeat
from ppinot4py.model import RuntimeState, TimeInstantCondition, AppliesTo
from ppinot4py.computers import condition_computer


#business = model.BusinessDuration(
#        business_start = time(7,0,0),
#        business_end = time(17,0,0),
#        weekend_list = [5,6],
#        holiday_list = pyholidays.ES(prov ='AN'),
#        unit_hour = 'hour'
#)

#timeMeasureCyclic = model.TimeMeasure(
#        from_condition='`concept:name` == "Create Fine"', 
#        to_condition='`concept:name` == "Send Fine"', 
#        single_instance_agg_function='SUM',
#        time_measure_type='CYCLIC',
#       business_duration = business
#       )

# Loads the event log
#log = pm4py.read_xes('Road_Traffic_Fine_Management_Process.xes')

# Transforms the event log into a pandas dataframe
#df = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)

# Converts the timestamp column into a timestamp
#df['time:timestamp'] = pd.to_datetime(df['time:timestamp'], utc=True)

# Computes the time between activity Create Fine and activity Send Fine
#tm = model.TimeMeasure('`concept:name` == "Create Fine"', '`concept:name` == "Send Fine"')
#timeMeasureCyclic2 = model.TimeMeasure(
#        from_condition='`concept:name` == "Create Fine"', 
#        to_condition='`concept:name` == "Send Fine"', 
#        business_duration = business
#        )
#result = ppinot4py.measure_computer(timeMeasureCyclic2, df)
#print(result)

#result = ppinot4py.measure_computer(timeMeasureCyclic, df)

#print(result)

# IdCase1 = '1-364285768'
# IdCase2 = '2-364285768'
    
# time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
# time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
# time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
# time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
# time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
# time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
# time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
# time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)


# data = {'case:concept:name':[IdCase2, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase2, IdCase1], 
#         'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
#         'concept:name': ['case11', 'case1', 'case2', 'case3', 'case4', 'case12', 'case13', 'case5'],
#         'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}

# dataframeLinear = pd.DataFrame(data)

# cond = TimeInstantCondition("'case1'", AppliesTo.ACTIVITY, "'Awaiting Assignment'")
#result = condition_computer(dataframeLinear, "case:concept:name", cond, 'lifecycle:transition', 'concept:name')

# business = model.BusinessDuration(
#         business_start = time(7,0,0),
#         business_end = time(17,0,0),
#         weekend_list = [5,6],
#         holiday_list = pyholidays.ES(prov ='AN'),
#         unit_hour = 'hour'
#     )
    
# timeMeasureCyclic = model.TimeMeasure(
# from_condition='`lifecycle:transition` == "In Progress"', 
# to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
# single_instance_agg_function='AVG',
# time_measure_type='CYCLIC',
# business_duration = business)

# IdCase1 = '1-364285768'
# IdCase2 = '2-364285768'

# time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
# time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
# time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
# time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
# time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
# time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
# time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
# time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)

# timeResult = datetime.timedelta(days= 69, hours=20, minutes=40, seconds=6) 

# data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
#         'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
#         'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}

# dataframeLinear = pd.DataFrame(data)
# var = ppinot4py.measure_computer(timeMeasureCyclic, dataframeLinear).iloc[0]
# print(var)
# print(timeResult)

# precondition = TimeInstantCondition("'Awaiting Assignment'", AppliesTo.ACTIVITY, "'Queued'")

# log = LogConfiguration(
# id_case = 'case:concept:name', 
# time_column = 'time:timestamp', 
# transition_column = 'lifecycle:transition:transition', 
# activity_column = 'concept:name:non:predefined')

# dataMeasure = DataMeasure(
#         data_content_selection="lifecycle:transition:transition", 
#         precondition = precondition,
#         first=False)

# IdCase1 = '1-364285768'
# IdCase2 = '1-364285769'

# data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
#         'concept:name:non:predefined': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
#         'lifecycle:transition:transition': ['In Progress', 'Awaiting Assignment','Completed', 'Awaiting Assignment','Completed']}
        
# dataframeLinear = pd.DataFrame(data)

# result = measure_computer(dataMeasure, dataframeLinear, log)


# print(result)

IdCase1 = '1-364285768'
IdCase2 = '2-364285768'
IdCase3 = '3-364285768'
IdCase4 = '4-364285768'
IdCase5 = '5-364285768'


time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
time3 = datetime.datetime(2012, 4, 6, 16, 44, 7)
time4 = datetime.datetime(2013, 4, 6, 16, 44, 7)
time5 = datetime.datetime(2014, 5, 6, 16, 44, 7)
time6 = datetime.datetime(2015, 6, 6, 16, 44, 7)
time7 = datetime.datetime(2016, 6, 6, 16, 44, 7)
time8 = datetime.datetime(2017, 6, 6, 16, 44, 7)
time9 = datetime.datetime(2018, 6, 6, 16, 44, 7)
time10 = datetime.datetime(2040, 10, 21, 16, 44, 7)


data = {'case:concept:name':[IdCase1, IdCase1, IdCase2, IdCase2, IdCase3, IdCase3, IdCase4, IdCase4, IdCase5, IdCase5], 
        'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8, time9, time10],
        'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment']}

dataframeLinear = pd.DataFrame(data)

timeMeasureLinearA = TimeMeasure(
from_condition='`lifecycle:transition` == "In Progress"',
to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
first_to=True)

rolling = RollingWindow(
      window = 2
)

aggregatedMeasure = AggregatedMeasure(
base_measure=timeMeasureLinearA, 
single_instance_agg_function='MAX')

var = measure_computer(aggregatedMeasure, dataframeLinear, LogConfiguration(), time_grouper=pd.Grouper(freq='2Y'), time_rolling=rolling)

#var[0] = datetime.timedelta(seconds=math.ceil(var[0].total_seconds()))
#var[1] = datetime.timedelta(seconds=math.ceil(var[1].total_seconds()))

print(var)
