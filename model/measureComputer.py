from measures import base
from computers.countComputer import countCompute
from computers.dataComputer import dataCompute
from computers.timeComputerGeneric import timeCompute
from computers.aggregatedComputer import aggregatedCompute

def measureComputer(measure, dataframe, id_case = 'case_concept_name', time_column = 'time_timestamp'):

    # Need to change ":" for "_" because ".query" put nervous with ":"
    dataframe.columns = [column.replace(":", "_") for column in dataframe.columns]

    # Evaluation wich kind of measure is
    if(type(measure) == base.CountMeasure):
        computer = countCompute(dataframe,measure, id_case)
    if(type(measure) == base.DataMeasure):
        computer = dataCompute(dataframe,measure, id_case)
    if(type(measure) == base.TimeMeasure):
        computer = timeCompute(dataframe,measure, id_case, time_column)
    if(type(measure) == base.aggregatedMeasure):
        computer = aggregatedCompute(dataframe, measure, id_case, time_column)
    return computer



