import json
import os
import sys
import time
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from condition.Condition import TimeInstantCondition
from state.RunTimeState import DataObjectState
from measures.base import CountMeasure, DataMeasure, aggregatedMeasure, TimeMeasure, derivedMeasure
from measureComputer import measureComputer
from timeGrouper import grouper

# Creating the .csv in case is the first time we run the program
if(os.path.exists("log_in_csv.csv") == False):
    log = xes_import_factory.apply('bpi_challenge_2013_incidents.xes')
    csv_exporter.export(log, "log_in_csv.csv")

# Loading .csv in dataframe
dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("log_in_csv.csv"))

# Count measure used 
countStateCount = DataObjectState("lifecycle_transition == 'Closed'")
countConditionCount = TimeInstantCondition(countStateCount)
countMeasureCount = CountMeasure(countConditionCount)

# Data measure used
countState = DataObjectState("org_group == 'V5 3rd'")
countCondition = TimeInstantCondition(countState)
countMeasure = CountMeasure(countCondition)
dataMeasure = DataMeasure("lifecycle_transition", countMeasure, True)

# Time measure used 
#(fromCondition, toCondition,  timeMeasureType = 'Linear', singleInstanceAggFunction = 'SUM', firstTo = 'False', precondition = '')

countStateTimeA = DataObjectState('lifecycle_transition == "In Progress"')
countConditionTimeA = TimeInstantCondition(countStateTimeA)
countMeasureTimeA = CountMeasure(countConditionTimeA)

countStateTimeB = DataObjectState('lifecycle_transition == "Closed"')
countConditionTimeB = TimeInstantCondition(countStateTimeB)
countMeasureTimeB = CountMeasure(countConditionTimeB)

countStateTimeC = DataObjectState('lifecycle_transition == "Awaiting Assignment"')
countConditionTimeC = TimeInstantCondition(countStateTimeC)
countMeasureTimeC = CountMeasure(countConditionTimeC)

timeMeasureCyclic = TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC')


timeMeasureLinearA = TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'LINEAR')
timeMeasureLinearB = TimeMeasure(countMeasureTimeB, countMeasureTimeA, 'LINEAR')
timeMeasureLinearC = TimeMeasure(countMeasureTimeA, countMeasureTimeC, 'LINEAR')


              
#baseMeasure = measureComputer(timeMeasureLinear, dataframe)
timeGrouper = grouper('60s')
aggregatedCom = aggregatedMeasure(timeMeasureLinearA, '', 'SUM', timeGrouper)

measure_dictionary = {'ProgressCount': timeMeasureLinearA, 'ClosedCount': timeMeasureLinearB, 'AwwaitingCount': timeMeasureLinearC}

#measure_dictionary = {'ProgressCount': countMeasureTimeA, 'ClosedCount': countMeasureTimeB, 'AwwaitingCount': countMeasureTimeC}

derivedMeasure = derivedMeasure('(ProgressCount + ClosedCount) / AwwaitingCount', measure_dictionary)

# Call to the function and count time
start_time = time.process_time()
print(measureComputer(derivedMeasure, dataframe))
print("--- %s seconds ---" % (time.process_time() - start_time))