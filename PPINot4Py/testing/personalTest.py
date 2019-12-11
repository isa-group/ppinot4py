import time

from PPINot4Py import timeGrouper, dataframeImporter, computer
from PPINot4Py import computer
from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base


# Loading .csv in dataframe
dataframe = dataframeImporter.importer('bpi_challenge_2013_incidents.xes')


# Data measure used
countState = DataObjectState.DataObjectState("concept_name == 'Queued'")
countCondition = Condition.TimeInstantCondition(countState)
countMeasure = base.CountMeasure(countCondition)
dataMeasure = base.DataMeasure("lifecycle_transition", countMeasure, False)


# Time measure used 
#(fromCondition, toCondition,  timeMeasureType = 'Linear', singleInstanceAggFunction = 'SUM', firstTo = 'False', precondition = '')
countStateTimeA = DataObjectState.DataObjectState('lifecycle_transition == "In Progress"')
countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
countMeasureTimeA = base.CountMeasure(countConditionTimeA)

countStateTimeB = DataObjectState.DataObjectState('lifecycle_transition == "Closed"')
countConditionTimeB = Condition.TimeInstantCondition(countStateTimeB)
countMeasureTimeB = base.CountMeasure(countConditionTimeB)

countStateTimeC = DataObjectState.DataObjectState('lifecycle_transition == "Awaiting Assignment"')
countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
countMeasureTimeC = base.CountMeasure(countConditionTimeC)


timeMeasureLinearA = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB)
timeMeasureLinearB = base.TimeMeasure(countMeasureTimeB, countMeasureTimeA)
timeMeasureLinearC = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC)

timeMeasureCyclic = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC', 'SUM')

    
# Aggregated Measure baseMeasure = measureComputer(timeMeasureLinear, dataframe)
timeGrouper = timeGrouper.grouper('60s')
aggregatedMeasure = base.aggregatedMeasure(timeMeasureLinearA, '', 'SUM', timeGrouper)


# Derived Measure
measure_dictionary = {'ProgressCount': timeMeasureLinearA, 'ClosedCount': timeMeasureLinearB, 'AwwaitingCount': timeMeasureLinearC}
derivedMeasure = base.derivedMeasure('(ProgressCount + ClosedCount) / sqrt(AwwaitingCount)', measure_dictionary)

# Call to the function and count time
start_time = time.process_time()
print(computer.measureComputer(derivedMeasure, dataframe))
print("--- %s seconds ---" % (time.process_time() - start_time))