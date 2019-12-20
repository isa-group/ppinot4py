import datetime as dt
import pandas as pd
import numpy as np
import datetime
from PPINot4Py.condition.conditionChooser import conditionChooser
from PPINot4Py.timeFunctions.filtersAndCalcs import *

pd.options.mode.chained_assignment = None

def timeCompute(dataframe, condition, id_case, time_column):

    operation = condition.singleInstanceAggFunction
    precondition = condition.precondition
    timeMeasureType = condition.timeMeasureType
    fromCondition = condition.fromCondition
    toCondition = condition.toCondition
    isFirst = condition.firstTo
    dataframeToWork = dataframe.copy()
    
    # Initial precondition
    if(precondition != ""):
        dataframeToWork = dataframeToWork.query(precondition)
   
    if(timeMeasureType == 'LINEAR'): 
  
        A_condition = conditionChooser(dataframeToWork, id_case, fromCondition)
        B_condition = conditionChooser(dataframeToWork, id_case, toCondition)
        
        filtered_dataframe_A = dataframeToWork[A_condition]

        fromValues = filtered_dataframe_A.groupby(id_case)[time_column].first()
        
        if(isFirst):
            
            dataframeToWork.loc[A_condition == True, 'simple_cond'] = 'A'
            dataframeToWork.loc[B_condition == True, 'simple_cond'] = 'B'
            
            dataframeToWork = dataframeToWork.dropna()
            
            dataframeToWork['simple_cond_next'] = dataframeToWork['simple_cond'].shift(-1)
            dataframeToWork['id_pre'] = dataframeToWork[id_case].shift(+1)
            
            dataframeToWork['b_appearance'] = dataframeToWork['simple_cond_next'] == "B"
            dataframes_with_B_condition = dataframeToWork.groupby(id_case)['b_appearance'].max()
            
            first_filtered_dataframe = B_and_A_and_Id_equals_idPre(dataframeToWork, id_case)
            
            second_filtered_dataframe = first_filtered_dataframe[(first_filtered_dataframe['simple_cond'] == "B") == True]
                
            toValues = second_filtered_dataframe.groupby(id_case)[time_column].first()
            
            dataframeToWork = dataframeToWork.drop(columns=['simple_cond'])
               
        else:
            finalDataframeB = dataframeToWork[B_condition]
            
            toValues = finalDataframeB.groupby(id_case)[time_column].last()
        
        finalResult = linear_calculation(fromValues, toValues, id_case)
    
    elif(timeMeasureType == 'CYCLIC'):

        A_condition = conditionChooser(dataframeToWork, id_case, fromCondition)
        B_condition = conditionChooser(dataframeToWork, id_case, toCondition) 
        
        # Create new Column with A and B alias for our condition, so we can generalice
        dataframeToWork.loc[A_condition == True, 'simple_cond'] = 'A'
        dataframeToWork.loc[B_condition == True, 'simple_cond'] = 'B'
        

        # Drop NaN values to filter all the values that are different from our conditions
        dataframeToWork = dataframeToWork.dropna()
        
        # We Shift the needed data for the filters
        dataframeToWork = shiftConditions(dataframeToWork, id_case)

        # Firts letter is actual condition, second is next condition, thirth is pre condition
        first_filter_dataframe = A_and_A_and_A_equalIds(dataframeToWork)
        first_filter_dataframe['time_timestamp_next'] =  first_filter_dataframe[time_column].shift(-1)
        
        second_filter_dataframe = B_and_A_and_A(first_filter_dataframe)

        third_filter_dataframe = condition_equal_preCondition_or_id_equal_nextId_or_id_notEqual_preId(second_filter_dataframe, id_case)
        third_filter_dataframe['cusom_eva'] = third_filter_dataframe[id_case].shift(-1)

        fourth_filter_dataframe = A_and_A_and_B_customEva_notEqual_Id(third_filter_dataframe, id_case)
        
        fifth_filter_dataframe = A_and_B_and_B_and_id_notEquals_IdNext(fourth_filter_dataframe, id_case)
        
        sixth_filter_dataframe = B_and_B(fifth_filter_dataframe)
        sixth_filter_dataframe['b_appearance'] = sixth_filter_dataframe['simple_cond_next'] == "B"
        
        dataframes_with_B_condition = sixth_filter_dataframe.groupby(id_case)['b_appearance'].max()
        
        # We calculate the time elapsed in each of the filtered rows     
        time_calculated_dataframe = timeCalculation_and_TimeConversion(sixth_filter_dataframe, time_column)
    
       
        if(operation == 'SUM'):
            finalResultAlmost = time_calculated_dataframe.groupby(id_case)['time_elapsed_seconds'].sum()
        
            finalResult = convertToTimeAndFinalFilter(finalResultAlmost, dataframes_with_B_condition)

        elif(operation == "MIN"):
            finalResultAlmost = time_calculated_dataframe.groupby(id_case)['time_elapsed_seconds'].min()
            
            finalResult = convertToTimeAndFinalFilter(finalResultAlmost, dataframes_with_B_condition)

        elif(operation == "MAX"):
            finalResultAlmost = time_calculated_dataframe.groupby(id_case)['time_elapsed_seconds'].max()
            
            finalResult = convertToTimeAndFinalFilter(finalResultAlmost, dataframes_with_B_condition)

        elif(operation == "AVG"):
            finalResultAlmost = time_calculated_dataframe.groupby(id_case)['time_elapsed_seconds'].mean()
            
            finalResult = convertToTimeAndFinalFilter(finalResultAlmost, dataframes_with_B_condition)

        # Timestamp is returned in seconds, to convert to timeDelta, (lambda x: datetime.timedelta(seconds = x))
        elif(operation == "GROUPBY"):
            finalResult = time_calculated_dataframe.groupby(id_case)
            
        dataframeToWork = dataframeToWork.drop(columns=['simple_cond'])
        
    #finalResult = pd.Series(finalResult['data'])
    

    
    return finalResult