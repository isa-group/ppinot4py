from PPINot4Py.measures.base import CountMeasure, DataMeasure, TimeMeasure, aggregatedMeasure, derivedMeasure
from PPINot4Py.computers.countComputer import countCompute
from PPINot4Py.computers.dataComputer import dataCompute
from PPINot4Py.computers.timeComputerGeneric import timeCompute
from PPINot4Py.computers.aggregatedComputer import aggregatedCompute
from PPINot4Py.computers.derivedComputer import derivedCompute

def measureComputer(measure, dataframe, id_case = 'case_concept_name', time_column = 'time_timestamp'):
    """ General computer.
    
    Args:
    
            - measure: Base measure, it will call different computers depending of the type.
            - dataframe: Base dataframe we want to use.
            - idCase (optional): ID column of your dataframe (By defect is 'case:concept:name').
            - timeColumn (optional): Timestamp column of your dataframe (By defect is 'time:timestamp').
            
    Returns:
    
            - Series: Series with pairs of ID - Data

    """
    try:
        # Need to change ":" for "_" because ".query" put nervous with ":"
        dataframe.columns = [column.replace(":", "_") for column in dataframe.columns]
        # Evaluation wich kind of measure is
        if(type(measure) == CountMeasure):
            computer = countCompute(dataframe,measure, id_case)
        if(type(measure) == DataMeasure):
            computer = dataCompute(dataframe,measure, id_case)
        if(type(measure) == TimeMeasure):
            computer = timeCompute(dataframe,measure, id_case, time_column)
        if(type(measure) == aggregatedMeasure):
            computer = aggregatedCompute(dataframe, measure, id_case, time_column)
        if(type(measure) == derivedMeasure):
            computer = derivedCompute(dataframe, measure, id_case, time_column)
        return computer
    except ValueError:
        return "ERROR: A value in the measure wasn't correctly defined"



