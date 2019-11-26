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

        dataframe = dataframe.query(fromCondition + ' or ' + toCondition)

        var_A = dataframe.query(fromCondition)
        var_B = dataframe.query(toCondition)

        #dataframe['lifecycle_transition_next'] = dataframe[columnToCompute].shift(-1)
        #dataframe['lifecycle_transition_pre'] = dataframe[columnToCompute].shift(+1)
        
        dataframe['id_next'] = dataframe[id_case].shift(-1)
        dataframe['id_pre'] = dataframe[id_case].shift(+1)

        dataframe["partial_evaluation_A"] = dataframe.index.isin(var_A.index)

        #dataframe['cumSum'] = dataframe["partial_evaluation_B"].cumsum()
        #dataframe['cumSum_shifted'] = dataframe['cumSum'].shift(-1) Still not needed

        dataframe.loc[dataframe['partial_evaluation_A'] == True, 'adapted_condition'] = False
        dataframe['adapted_condition'] = dataframe['adapted_condition'].fillna(True)

        dataframe.loc[dataframe['partial_evaluation_A'] == True, 'adapted_condition_eva'] = 'A'
        dataframe['adapted_condition_eva'] = dataframe['adapted_condition_eva'].fillna('B')

        dataframe['cumsum_B'] = dataframe['adapted_condition'].cumsum()

        #dataframe['condition_shift_next'] = dataframe['adapted_condition'].shift(-1)
        dataframe['consum_pre'] = dataframe['cumsum_B'].shift(+1)
        dataframe['consum_pre'] = dataframe['consum_pre'].fillna(0)

        
        dataframe['cumsum_result'] = dataframe['cumsum_B'] - dataframe['consum_pre']
        dataframe['cumsum_result'] =  dataframe['cumsum_result'].fillna(0)
        dataframe['adapted_condition_eva_next'] = dataframe['adapted_condition_eva'].shift(-1)
        dataframe['adapted_condition_eva_pre'] = dataframe['adapted_condition_eva'].shift(+1)

        

        fafa = dataframe[dataframe['case_concept_name'] == '1-364285768']
        print(fafa[['adapted_condition_eva','adapted_condition_eva_next', 'adapted_condition_eva_pre', 'cumsum_B', 'consum_pre', 'cumsum_result']])


        dada = dataframe.groupby([id_case, 'consum_pre']).sum()
        print(dada)
        #dataframe['final_evaluation'] = ((dataframe["partial_evaluation_A"] == True) &
        #                                (dataframe["partial_evaluation_B"] == False) & 
        #                                    (dataframe[id_case] == dataframe['id_next']))

        #filteredData = dataframe[dataframe['final_evaluation'] == True]
        #fafa = filteredData[filteredData['case_concept_name'] == '1-364285768']
        #print(fafa[['lifecycle_transition', 'lifecycle_transition_next', 'lifecycle_transition_pre', id_case, 'id_next', 'id_pre']])
        

        # We apply shift to each column needed in the verification filter
        

        definitiveDataframe = (dataframe[
            (dataframe['adapted_condition_eva'] == 'A') & 
                (dataframe['adapted_condition_eva_next'] == 'A') & 
                    (dataframe['adapted_condition_eva_pre'] == 'A') &
                        (dataframe['id_next'] == dataframe['id_pre']) == False])

        definitiveDataframe['time_timestamp_next'] =  definitiveDataframe[time_column].shift(-1)
        
        finalFilteredDataframe_2 = (definitiveDataframe[
            (definitiveDataframe['adapted_condition_eva'] == "B") & 
                (definitiveDataframe['adapted_condition_eva_next'] == 'A') & 
                    (definitiveDataframe['adapted_condition_eva_pre'] == 'A') == False])  

        finalFilteredDataframe = (finalFilteredDataframe_2[(finalFilteredDataframe_2['adapted_condition_eva'] != 
                    finalFilteredDataframe_2['adapted_condition_eva_pre']) |
                        (((finalFilteredDataframe_2['id_next'] == finalFilteredDataframe_2[id_case]))) |
                            (((finalFilteredDataframe_2['id_pre'] != finalFilteredDataframe_2[id_case])))])

        finalFilteredDataframe['cusom_eva'] = finalFilteredDataframe[id_case].shift(-1)
        
        finalFilteredDataframe_1 = (finalFilteredDataframe[
            (finalFilteredDataframe['adapted_condition_eva'] == 'A') & 
                (finalFilteredDataframe['adapted_condition_eva_next'] == 'A') & 
                    (finalFilteredDataframe['adapted_condition_eva_pre'] == "B") &
                        (finalFilteredDataframe['cusom_eva'] != finalFilteredDataframe[id_case]) == False])

        finalFilteredDataframe_1['b_appearance'] = finalFilteredDataframe_1['adapted_condition_eva_next'] == "B"
        finalRealtiveResilt = finalFilteredDataframe_1.groupby(id_case)['b_appearance'].max()
        
        # We calculate the time elapsed in each of the filtered rows     
        finalFilteredDataframe_1['time_elapsed'] = finalFilteredDataframe_1['time_timestamp_next'] - finalFilteredDataframe_1[time_column]
       
        if(operation == 'SUM'):
            finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed'].dt.total_seconds()
            finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed_seconds'].fillna(0).astype(float)
            
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].sum()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))

        elif(operation == "MIN"):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed'].min()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]

        elif(operation == "MAX"):
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed'].max()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]

        elif(operation == "AVG"):
            finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed'].dt.total_seconds()
            finalFilteredDataframe_1['time_elapsed_seconds'] = finalFilteredDataframe_1['time_elapsed_seconds'].fillna(0).astype(float)
            
            finalResultAlmost = finalFilteredDataframe_1.groupby(id_case)['time_elapsed_seconds'].mean()
            finalResult = finalResultAlmost[finalRealtiveResilt == True]
            finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
           
    return None