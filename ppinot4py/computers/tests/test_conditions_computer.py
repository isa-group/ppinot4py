
import datetime
import pandas as pd
from ppinot4py.model import RuntimeState, TimeInstantCondition, AppliesTo
from ppinot4py.computers import condition_computer

def test_time_instant_process_start():
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)

    cond = TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)
    result = condition_computer(dataframeLinear, "case:concept:name", cond)

    result_expected = [True, False, False, False, False, False, True, False]

    assert (result.values == result_expected).all()

def test_time_instant_process_end():
    IdCase1 = '1-364285768'
    IdCase2 = '2-364285768'
    
    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
    time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
    time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
    time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
    
    data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
            'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
            'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
    
    dataframeLinear = pd.DataFrame(data)

    cond = TimeInstantCondition(RuntimeState.END, AppliesTo.PROCESS)
    result = condition_computer(dataframeLinear, "case:concept:name", cond)

    result_expected = [False, False, False, False, False, False, True, True]

    assert (result.values == result_expected).all()
