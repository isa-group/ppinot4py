import datetime as dt
import pandas as pd
import numpy as np
import datetime
from functionsFrame import auxFunctions

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
    
    # Initial precondition
    if(precondition != ""):
        dataframe = dataframe.query(precondition)
   
    if(timeMeasureType == 'LINEAR'): 

        A_condition = auxFunctions.timeInstantConditionFunction(dataframe, id_case, fromCondition)
        B_condition = auxFunctions.timeInstantConditionFunction(dataframe, id_case, toCondition)

        dataframe.loc[A_condition == True, 'simple_cond'] = 'A'
        dataframe.loc[B_condition == True, 'simple_cond'] = 'B'
        dataframe = dataframe.dropna()
       
        # We take first value of "FROM" dataframe
        fromValues = dataframe.groupby(id_case).first()[time_column].reindex(dataframe[id_case])

        # We evaluate isFirst condition and we take first or last condition
        if(isFirst):
            toValues = dataframe.groupby(id_case).first()[time_column].reindex(dataframe[id_case])
                
        else:
            toValues = dataframe.groupby(id_case).last()[time_column].reindex(dataframe[id_case])

        # We calculate the date difference
        finalResult = toValues - fromValues

        # We group by and we apply the max function that have no impact in our performance
        finalResult = finalResult.groupby(id_case).max()
    
    elif(timeMeasureType == 'CYCLIC'):

        #Cambiar comienzo para TIMEINSTANTCONDITION
        A_condition = auxFunctions.timeInstantConditionFunction(dataframe, id_case, fromCondition)
        B_condition = auxFunctions.timeInstantConditionFunction(dataframe, id_case, toCondition)

        dataframe.loc[A_condition == True, 'simple_cond'] = 'A'
        dataframe.loc[B_condition == True, 'simple_cond'] = 'B'

        dataframe = dataframe.dropna()

        # We Shift the needed data for the filters
        dataframe['simple_cond_next'] = dataframe['simple_cond'].shift(-1)
        dataframe['simple_cond_pre'] = dataframe['simple_cond'].shift(+1)
        dataframe['id_next'] = dataframe[id_case].shift(-1)
        dataframe['id_pre'] = dataframe[id_case].shift(+1)
    
    
        definitiveDataframe = (dataframe[
            (dataframe['simple_cond'] == 'A') & 
                (dataframe['simple_cond_next'] == 'A') & 
                    (dataframe['simple_cond_pre'] == 'A') &
                        (dataframe['id_next'] == dataframe['id_pre']) == False])

        definitiveDataframe['time_timestamp_next'] =  definitiveDataframe[time_column].shift(-1)
        
        finalFilteredDataframe_2 = (definitiveDataframe[
            (definitiveDataframe['simple_cond'] == "B") & 
                (definitiveDataframe['simple_cond_next'] == 'A') & 
                    (definitiveDataframe['simple_cond_pre'] == 'A') == False])  


        finalFilteredDataframe = (finalFilteredDataframe_2[(finalFilteredDataframe_2['simple_cond'] != 
                    finalFilteredDataframe_2['simple_cond_pre']) |
                        (((finalFilteredDataframe_2['id_next'] == finalFilteredDataframe_2[id_case]))) |
                            (((finalFilteredDataframe_2['id_pre'] != finalFilteredDataframe_2[id_case])))])


        finalFilteredDataframe['cusom_eva'] = finalFilteredDataframe[id_case].shift(-1)

        finalFilteredDataframe_1 = (finalFilteredDataframe[
            (finalFilteredDataframe['simple_cond'] == 'A') & 
                (finalFilteredDataframe['simple_cond_next'] == 'A') & 
                    (finalFilteredDataframe['simple_cond_pre'] == "B") & 
                        (finalFilteredDataframe['cusom_eva'] != finalFilteredDataframe[id_case]) == False]) 

        finalFilteredDataframe_1['b_appearance'] = finalFilteredDataframe_1['simple_cond_next'] == "B"
        finalRealtiveResilt = finalFilteredDataframe_1.groupby(id_case)['b_appearance'].max()
        
        # We calculate the time elapsed in each of the filtered rows     
        finalFilteredDataframe_1['time_elapsed'] = finalFilteredDataframe_1['time_timestamp_next'] - finalFilteredDataframe_1[time_column]

        finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed'].dt.total_seconds()
        finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed_seconds'].fillna(0).astype(float)
       
        if(operation == 'SUM'):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].sum()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))

        elif(operation == "MIN"):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].min()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))

        elif(operation == "MAX"):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].max()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))

        elif(operation == "AVG"):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].mean()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))

        #Timestamp is returned in seconds, to convert to timeDelta, (lambda x: datetime.timedelta(seconds = x))
        elif(operation == "GROUPBY"):
            finalResult = finalFilteredDataframe_1.groupby(id_case)
           
    return finalResult