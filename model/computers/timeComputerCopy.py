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

        dfToList = filteredDataframe[filteredDataframe['case_concept_name'] == '1-740865969']
        print(dfToList[[columnToCompute, 'lifecycle_transition_next', 'lifecycle_transition_pre']])

        filteredDataframe = (filteredDataframe[(filteredDataframe['lifecycle_transition_pre'] != filteredDataframe[columnToCompute]) &
                                                    (((filteredDataframe[columnToCompute] == 'In Progress') & (filteredDataframe['lifecycle_transition_next'] == 'In Progress') & (filteredDataframe['lifecycle_transition_pre'] == "Awaiting Assignment")) == False) & 
                                                     (((filteredDataframe[columnToCompute] == 'Awaiting Assignment') & (filteredDataframe['lifecycle_transition_next'] == 'In Progress') & (filteredDataframe['lifecycle_transition_pre'] != "Awaiting Assignment")) == False)])
        #print(filteredDataframe)


        #finalFilteredDataframe = (filteredDataframe[(filteredDataframe['lifecycle_transition_pre'] != filteredDataframe[columnToCompute]) &
                                        #(((filteredDataframe[columnToCompute] == 'In Progress') & (filteredDataframe['lifecycle_transition_next'] == 'In Progress') & 
                                        #    (filteredDataframe['lifecycle_transition_pre'] == "Awaiting Assignment")) == False) & 
                                        #(((filteredDataframe[columnToCompute] == 'Awaiting Assignment') & (filteredDataframe['lifecycle_transition_next'] == 'In Progress') & 
                                        #    (filteredDataframe['lifecycle_transition_pre'] != "Awaiting Assignment")) == False) &
                                        #(((filteredDataframe['id_next'] != filteredDataframe[id_case]))) |
                                        #(((filteredDataframe['id_pre'] != filteredDataframe[id_case])))])
        
       
        #print(filteredDataframe)
        #dfToList = filteredDataframe[filteredDataframe['case_concept_name'] == '1-364285768']
        #print(dfToList[[columnToCompute, 'lifecycle_transition_next', 'lifecycle_transition_pre', 'validation_column']])
        # STILLS NEED CHANGE ----------------------------------------------------------------------------------------------------------------------------------
        # Maybe follow countComputer with Query + ISIN
        finalFilteredDataframe = (filteredDataframe[(filteredDataframe['id_next'] == filteredDataframe[id_case]) == True])

        
        
        #dfToList = finalFilteredDataframe[finalFilteredDataframe['case_concept_name'] == '1-364285768']
        #print(dfToList[[columnToCompute, 'lifecycle_transition_next', 'validation_column']])
        #------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        # We calculate the time elapsed in each of the filtered rows
        finalFilteredDataframe['time_elapsed'] = finalFilteredDataframe['time_timestamp_next'] - finalFilteredDataframe[time_column]

        

        if(operation == 'SUM'):
            finalResult = finalFilteredDataframe.groupby(id_case)['time_elapsed'].sum()
            
        elif(operation == "MIN"):
            finalResult = finalFilteredDataframe[finalFilteredDataframe['lifecycle_transition_next'] == 'Awaiting Assignment'].groupby(id_case)['time_elapsed'].min()

        elif(operation == "MAX"):
            finalResult = finalFilteredDataframe[finalFilteredDataframe['lifecycle_transition_next'] == 'Awaiting Assignment'].groupby(id_case)['time_elapsed'].max()

        elif(operation == "AVG"):
            finalFilteredDataframe['time_elapsed_seconds'] = finalFilteredDataframe['time_elapsed'].dt.total_seconds()
            result = finalFilteredDataframe[finalFilteredDataframe['lifecycle_transition_next'] == 'Awaiting Assignment'].groupby(id_case)['time_elapsed_seconds'].mean()
            finalResult = result.apply(lambda x: datetime.timedelta(seconds = x))
           
    return finalResult