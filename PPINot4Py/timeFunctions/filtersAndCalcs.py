import datetime
import pandas as pd
import numpy as np

def A_and_A_and_A_equalIds(dataframe):
    dataFrameResult = (dataframe[
            (dataframe['simple_cond'] == 'A') & 
                (dataframe['simple_cond_next'] == 'A') & 
                    (dataframe['simple_cond_pre'] == 'A') &
                        (dataframe['id_next'] == dataframe['id_pre']) == False])
    return dataFrameResult


def B_and_A_and_A(dataframe):
    dataFrameResult = (dataframe[
                (dataframe['simple_cond'] == "B") & 
                    (dataframe['simple_cond_next'] == 'A') & 
                        (dataframe['simple_cond_pre'] == 'A') == False])  
    return dataFrameResult    


def condition_equal_preCondition_or_id_equal_nextId_or_id_notEqual_preId(dataframe, id_case):
    dataFrameResult = (dataframe[(dataframe['simple_cond'] != dataframe['simple_cond_pre']) |
                            (((dataframe['id_next'] == dataframe[id_case]))) |
                                (((dataframe['id_pre'] != dataframe[id_case])))])
    return dataFrameResult
    
    
def A_and_A_and_B_customEva_notEqual_Id(dataframe, id_case):
    dataFrameResult = (dataframe[
                (dataframe['simple_cond'] == 'A') & 
                    (dataframe['simple_cond_next'] == 'A') & 
                        (dataframe['simple_cond_pre'] == "B") & 
                            (dataframe['cusom_eva'] != dataframe[id_case]) == False]) 
    return dataFrameResult


def A_and_B_and_B_and_id_notEquals_IdNext(dataframe,id_case):
    dataFrameResult = (dataframe[
                (dataframe['simple_cond'] == 'A') & 
                    (dataframe['simple_cond_next'] == 'B') & 
                        (dataframe['simple_cond_pre'] == "B") &
                            (dataframe[id_case] != dataframe['id_next']) == False])
    return dataFrameResult

def B_and_B(dataframe):
    dataFrameResult = (dataframe[(dataframe['simple_cond'] == 'B') & 
                        (dataframe['simple_cond_next'] == 'B') == False])
    return dataFrameResult


def convertToTimeAndFinalFilter(dataframe, daraframeWithBCondition):

    dataframe = dataframe[daraframeWithBCondition]
    dataframe = dataframe.apply(lambda x: datetime.timedelta(seconds = x))
    dataframe = dataframe.to_frame('data')
    
    dataframe = dataframe[dataframe['data'] > pd.Timedelta(0)]
    
    return dataframe

def B_and_A_and_Id_equals_idPre(dataframe, id_case):

    dataFrameResult = (dataframe[
                (dataframe['simple_cond'] == "B") & 
                    (dataframe['simple_cond_next'] == 'A') & 
                        (dataframe[id_case] != dataframe['id_pre']) == False]) 
    return dataFrameResult


def linear_calculation(fromValues, toValues, id_case):
    
    finalResultAlmost = toValues - fromValues   

    finalResultWithNaN = finalResultAlmost.groupby(id_case).max()
    finalResultWithNegatives = finalResultWithNaN.dropna()
    finalResultDataframe = finalResultWithNegatives.to_frame('data')
        
    finalResult = finalResultDataframe[finalResultDataframe['data'] > pd.Timedelta(0)]
    
    return finalResult

def shiftConditions(dataframe, id_case):
    dataframe['simple_cond_next'] = dataframe['simple_cond'].shift(-1)
    dataframe['simple_cond_pre'] = dataframe['simple_cond'].shift(+1)
    dataframe['id_next'] = dataframe[id_case].shift(-1)
    dataframe['id_pre'] = dataframe[id_case].shift(+1)
    
    return dataframe

def timeCalculation_and_TimeConversion(dataframe, time_column):
    
    dataframe['time_elapsed'] = dataframe['time_timestamp_next'] - dataframe[time_column]
    dataframe['time_elapsed_seconds'] = dataframe['time_elapsed'].dt.total_seconds()
    dataframe['time_elapsed_seconds'] = dataframe['time_elapsed_seconds'].fillna(0).astype(float)
    
    return dataframe
