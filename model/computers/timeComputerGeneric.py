import datetime as dt
import pandas as pd
import numpy as np
import datetime
from condition.conditionChooser import conditionChooser

pd.options.mode.chained_assignment = None

def timeCompute(dataframe, condition, id_case, time_column):

    operation = condition.singleInstanceAggFunction
    precondition = condition.precondition
    timeMeasureType = condition.timeMeasureType
    fromCondition = condition.fromCondition
    toCondition = condition.toCondition
    isFirst = condition.firstTo
    
    # Initial precondition
    if(precondition != ""):
        dataframe = dataframe.query(precondition)
   
    if(timeMeasureType == 'LINEAR'): 
  
        A_condition = conditionChooser(dataframe, id_case, fromCondition)
        B_condition = conditionChooser(dataframe, id_case, toCondition)
        
        finalDataframeA = dataframe[A_condition]

        fromValues = finalDataframeA.groupby(id_case)[time_column].first()
        
        if(isFirst):
            
            dataframe.loc[A_condition == True, 'simple_cond'] = 'A'
            dataframe.loc[B_condition == True, 'simple_cond'] = 'B'
            
            dataframe = dataframe.dropna()
            
            dataframe['simple_cond_next'] = dataframe['simple_cond'].shift(-1)
            dataframe['id_pre'] = dataframe[id_case].shift(+1)
            
            dataframe['b_appearance'] = dataframe['simple_cond_next'] == "B"
            finalRealtiveResilt = dataframe.groupby(id_case)['b_appearance'].max()
            
            finalFilteredDataframe = (dataframe[
            (dataframe['simple_cond'] == "B") & 
                (dataframe['simple_cond_next'] == 'A') & 
                    (dataframe[id_case] != dataframe['id_pre']) == False]) 
            
            finalFilteredDataframe_b = (finalFilteredDataframe[
                (finalFilteredDataframe['simple_cond'] == "B") == True]) 
                
            toValues = finalFilteredDataframe_b.groupby(id_case)[time_column].first()
               
        else:
            finalDataframeB = dataframe[B_condition]
            
            toValues = finalDataframeB.groupby(id_case)[time_column].last()

        # We calculate the date difference
        finalResultAlmost = toValues - fromValues     

        # We group by and we apply the max function that have no impact in our performance
        finalResultWithNaN = finalResultAlmost.groupby(id_case).max()
        finalResultWithNegatives = finalResultWithNaN.dropna()
        
        finalResultDataframe = finalResultWithNegatives.to_frame('data')
        
        finalResult = finalResultDataframe[finalResultDataframe['data'] > pd.Timedelta(0)]
    
    elif(timeMeasureType == 'CYCLIC'):

        #Cambiar comienzo para TIMEINSTANTCONDITION
        A_condition =  conditionChooser(dataframe, id_case, fromCondition)
        B_condition =  conditionChooser(dataframe, id_case, toCondition) 

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
        
        finalFilteredDataframe_1_BConditions_2 = (finalFilteredDataframe_1[
            (finalFilteredDataframe_1['simple_cond'] == 'A') & 
                (finalFilteredDataframe_1['simple_cond_next'] == 'B') & 
                   (finalFilteredDataframe_1['simple_cond_pre'] == "B") &
                        (finalFilteredDataframe_1[id_case] != finalFilteredDataframe_1['id_next']) == False])
        
        finalFilteredDataframe_1_BConditions = (finalFilteredDataframe_1_BConditions_2[
            (finalFilteredDataframe_1_BConditions_2['simple_cond'] == 'B') & 
                (finalFilteredDataframe_1_BConditions_2['simple_cond_next'] == 'B') == False])

        finalFilteredDataframe_1_BConditions['b_appearance'] = finalFilteredDataframe_1_BConditions['simple_cond_next'] == "B"
        finalRealtiveResilt = finalFilteredDataframe_1_BConditions.groupby(id_case)['b_appearance'].max()
        
        # We calculate the time elapsed in each of the filtered rows     
        finalFilteredDataframe_1_BConditions['time_elapsed'] = finalFilteredDataframe_1_BConditions['time_timestamp_next'] - finalFilteredDataframe_1_BConditions[time_column]

        finalFilteredDataframe_1_BConditions['time_elapsed_seconds'] = finalFilteredDataframe_1_BConditions['time_elapsed'].dt.total_seconds()
        finalFilteredDataframe_1_BConditions['time_elapsed_seconds'] = finalFilteredDataframe_1_BConditions['time_elapsed_seconds'].fillna(0).astype(float)
       
        if(operation == 'SUM'):
            finalResultAlmost = finalFilteredDataframe_1_BConditions.groupby(id_case)['time_elapsed_seconds'].sum()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
            finalResult = finalResult.to_frame('data')
        
            finalResult = finalResult[finalResult['data'] > pd.Timedelta(0)]

        elif(operation == "MIN"):
            finalResultAlmost = finalFilteredDataframe_1_BConditions.groupby(id_case)['time_elapsed_seconds'].min()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
            finalResult = finalResult.to_frame('data')
        
            finalResult = finalResult[finalResult['data'] > pd.Timedelta(0)]

        elif(operation == "MAX"):
            finalResultAlmost = finalFilteredDataframe_1_BConditions.groupby(id_case)['time_elapsed_seconds'].max()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
            finalResult = finalResult.to_frame('data')
        
            finalResult = finalResult[finalResult['data'] > pd.Timedelta(0)]

        elif(operation == "AVG"):
            finalResultAlmost = finalFilteredDataframe_1_BConditions.groupby(id_case)['time_elapsed_seconds'].mean()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
            finalResult = finalResult.to_frame('data')
        
            finalResult = finalResult[finalResult['data'] > pd.Timedelta(0)]

        #Timestamp is returned in seconds, to convert to timeDelta, (lambda x: datetime.timedelta(seconds = x))
        elif(operation == "GROUPBY"):
            finalResult = finalFilteredDataframe_1_BConditions.groupby(id_case)
            
    finalResult = pd.Series(finalResult['data'])
    
    return finalResult