from PPINot4Py.condition.conditionChooser import conditionChooser

def dataCompute(dataframe, condition, id_case):

    precondition = (condition.precondition.when)

    filtered_series = conditionChooser(dataframe, id_case, precondition)
    
    final_dataframe = dataframe[filtered_series]
   
    if(condition.first):
        result = final_dataframe.groupby(id_case)[condition.dataContentSelection].first()
    else:
        result = final_dataframe.groupby(id_case)[condition.dataContentSelection].last()

    return result