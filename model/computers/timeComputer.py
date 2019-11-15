import datetime as dt
import pandas as pd
import numpy as np
import datetime


pd.options.mode.chained_assignment = None

def timeCompute(dataframe, condition, id_case, time_column):

    operation = condition.singleInstanceAggFunction
    precondition = condition.precondition
    timeMeasureType = condition.timeMeasureType
    fromCondition = condition.fromCondition
    toCondition = condition.toCondition
    isFirst = condition.firstTo
    considerOnly = condition.considerOnly
    computeUnfinished = condition.computeUnfinished
    columnToCompute = condition.columnToCompute
    
    # Initial precondition
    if(precondition != ""):
        dataframe = dataframe.query(precondition)
   
    if(timeMeasureType == 'LINEAR'):

        #We apply from condition and to condition
        filteredDataframeFrom = dataframe.query(fromCondition)
        filteredDataframeTo = dataframe.query(toCondition)
       
        # We take first value of "FROM" dataframe
        fromValues = filteredDataframeFrom.groupby(id_case).first()[time_column].reindex(filteredDataframeFrom[id_case])

        # We evaluate isFirst condition and we take first or last condition
        if(isFirst):
            toValues = filteredDataframeTo.groupby(id_case).first()[time_column].reindex(filteredDataframeTo[id_case])
                
        else:
            toValues = filteredDataframeTo.groupby(id_case).last()[time_column].reindex(filteredDataframeTo[id_case])

        # We calculate the date difference
        finalResult = toValues - fromValues

        # We group by and we apply the max function that have no impact in our performance
        finalResult = finalResult.groupby(id_case).max()
    
    elif(timeMeasureType == 'CYCLIC'):
        # We apply the filter of "from or to" to retrieve in a new dataframe only the rows that satisfy the query
        filteredDataframe = dataframe.query(fromCondition + ' or ' + toCondition)

        # We apply shift to each column needed in the verification filter
        filteredDataframe['lifecycle_transition_next'] = filteredDataframe[columnToCompute].shift(-1)
        filteredDataframe['lifecycle_transition_pre'] = filteredDataframe[columnToCompute].shift(+1)
        filteredDataframe['time_timestamp_next'] =  filteredDataframe[time_column].shift(-1)
        filteredDataframe['id_next'] = filteredDataframe[id_case].shift(-1)
        filteredDataframe['id_pre'] = filteredDataframe[id_case].shift(+1)

        finalFilteredDataframe = (filteredDataframe[(filteredDataframe[columnToCompute] != 
                    filteredDataframe['lifecycle_transition_pre']) |
                        (((filteredDataframe['id_next'] == filteredDataframe[id_case]))) |
                            (((filteredDataframe['id_pre'] != filteredDataframe[id_case])))])

        finalFilteredDataframe_1 = (finalFilteredDataframe[
            (finalFilteredDataframe[columnToCompute] == 'In Progress') & 
                (finalFilteredDataframe['lifecycle_transition_next'] == 'In Progress') & 
                    (filteredDataframe['lifecycle_transition_pre'] == "Awaiting Assignment") == False])
           
        finalFilteredDataframe_2 = (finalFilteredDataframe_1[
            (finalFilteredDataframe_1[columnToCompute] == "Awaiting Assignment") & 
                (finalFilteredDataframe_1['lifecycle_transition_next'] == 'In Progress') & 
                    (finalFilteredDataframe_1['lifecycle_transition_pre'] == 'In Progress') == False])       

        definitiveDataframe = (finalFilteredDataframe_2[
            (finalFilteredDataframe_2[columnToCompute] == 'In Progress') & 
                (finalFilteredDataframe_2['lifecycle_transition_next'] == 'In Progress') & 
                    (finalFilteredDataframe_2['lifecycle_transition_pre'] == 'In Progress') &
                        (finalFilteredDataframe_2['id_next'] == finalFilteredDataframe_2['id_pre']) == False])

        definitiveDataframe['b_appearance'] = definitiveDataframe['lifecycle_transition_next'] == "Awaiting Assignment"
        finalRealtiveResilt = definitiveDataframe.groupby(id_case)['b_appearance'].max()
        
        # We calculate the time elapsed in each of the filtered rows     
        definitiveDataframe['time_elapsed'] = definitiveDataframe['time_timestamp_next'] - definitiveDataframe[time_column]
       
        if(operation == 'SUM'):
            finalResultAlmost = definitiveDataframe.groupby(id_case)['time_elapsed'].sum()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]

        elif(operation == "MIN"):
            finalResultAlmost = definitiveDataframe.groupby(id_case)['time_elapsed'].min()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]

        elif(operation == "MAX"):
            finalResultAlmost = definitiveDataframe.groupby(id_case)['time_elapsed'].max()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]

        elif(operation == "AVG"):
            definitiveDataframe['time_elapsed_seconds'] = definitiveDataframe['time_elapsed'].dt.total_seconds()
            definitiveDataframe['time_elapsed_seconds'] = definitiveDataframe['time_elapsed_seconds'].fillna(0).astype(float)
            
            finalResultAlmost = definitiveDataframe.groupby(id_case)['time_elapsed_seconds'].mean()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
           
    return finalResult