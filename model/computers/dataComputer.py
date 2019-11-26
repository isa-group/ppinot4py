from condition.conditionChooser import conditionChooser

def dataCompute(dataframe, condition, id_case):

    precond = (condition.precondition.when)

    auxDataframe = conditionChooser(dataframe, id_case, precond)
    
    finalDataframe = dataframe[auxDataframe]
   
    if(condition.first):
        value = finalDataframe.groupby(id_case)[condition.dataContentSelection].first()
    else:
        value = finalDataframe.groupby(id_case)[condition.dataContentSelection].last()

    return value