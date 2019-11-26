import json
import os
import sys
import time
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from condition.Condition import TimeInstantCondition
from state.RunTimeState import DataObjectState
from measures.base import CountMeasure, DataMeasure, aggregatedMeasure, TimeMeasure
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
countState = DataObjectState("concept_name == 'Queued'")
countCondition = TimeInstantCondition(countState)
countMeasure = CountMeasure(countCondition)
dataMeasure = DataMeasure("lifecycle_transition", countMeasure, False)

# Time measure used 
# Maybe add parameters by defect in some values
timeMeasureCyclic = TimeMeasure('lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Awaiting Assignment"', 'CYCLIC', 'SUM', '', '', '', False)

timeMeasureLinear = TimeMeasure('lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Closed"', 'LINEAR', 'AVG', '', '', '', False)

                    
baseMeasure = measureComputer(timeMeasureLinear, dataframe)
timeGrouper = grouper('2W')
aggregatedCom = aggregatedMeasure(baseMeasure, '', 'SUM', timeGrouper)


# Call to the function and count time
start_time = time.process_time()
print(measureComputer(aggregatedCom, dataframe))
print("--- %s seconds ---" % (time.process_time() - start_time))