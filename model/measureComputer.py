from measures import base
from computers.countComputer import countCompute
from computers.dataComputer import dataCompute
from computers.timeComputer import timeCompute

def measureComputer(measure, dataframe, id_case, time_column):

    #Verificar tipos que son correctos con asserts
    # Evaluation wich kind of measure is
    if(type(measure) == base.CountMeasure):
        computer = countCompute(dataframe,measure, id_case)
    if(type(measure) == base.DataMeasure):
        computer = dataCompute(dataframe,measure, id_case)
    if(type(measure) == base.TimeMeasure):
        computer = timeCompute(dataframe,measure, id_case, time_column)
    return computer



