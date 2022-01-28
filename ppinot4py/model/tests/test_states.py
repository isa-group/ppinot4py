import datetime
from ppinot4py.model import *
from ppinot4py.computers import *
import pandas as pd
import pytest


@pytest.fixture
def processDataframe():
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
    return dataframeLinear

@pytest.fixture
def activityDataframe():
    IdCase1 = '1-364285768'
    IdCase2 = '1-364285769'

    time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
    time1b = datetime.datetime(2010, 3, 31, 17, 32, 48)
    time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
    time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
    time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
    time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)

    data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase2], 
        'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Queued', 'Queued'],
        'time:timestamp': [time1, time1b, time2, time3, time4, time5],
        'lifecycle:transition': ['Assigned', 'Assigned', 'Awaiting Assignment','Completed', 'Assigned','Completed']}

    dataframeLinear = pd.DataFrame(data)

    return dataframeLinear


def test_complex_state_leads_to_process(processDataframe):

    timeInstant1 = TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)
    timeInstant2 = TimeInstantCondition(RuntimeState.END, AppliesTo.PROCESS)

    complex = ComplexState(timeInstant1, timeInstant2, Type.LEADSTO)

    result = condition_computer(processDataframe, "case:concept:name", complex, 'lifecycle:transition', 'concept:name')

    result_expected = [False, False, False, False, False, False, True, False]

    assert (result.values == result_expected).all()


def test_complex_state_leads_to_activity(activityDataframe):

    precondition1 = TimeInstantCondition(RuntimeState.START, AppliesTo.ACTIVITY, "'Queued'")
    precondition2 = TimeInstantCondition(RuntimeState.END, AppliesTo.ACTIVITY, "'Queued'")

    complex = ComplexState(precondition1, precondition2, Type.LEADSTO)

    result = condition_computer(activityDataframe, "case:concept:name", complex, 'concept:name', 'lifecycle:transition')

    result_expected = [False, False, False, True, False, False]

    assert (result.values == result_expected).all()


def test_complex_state_follows_process(processDataframe):

    timeInstant1 = TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)
    timeInstant2 = TimeInstantCondition(RuntimeState.END, AppliesTo.PROCESS)

    complex = ComplexState(timeInstant1, timeInstant2, Type.FOLLOWS)

    result = condition_computer(processDataframe, "case:concept:name", complex, 'lifecycle:transition', 'concept:name')

    result_expected = [False, False, False, False, False, False, False, True]

    assert (result.values == result_expected).all()


def test_complex_state_follows_process_recursive(processDataframe):

    timeInstant1 = TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)

    timeInstantComplex1 = TimeInstantCondition(RuntimeState.START, AppliesTo.PROCESS)
    timeInstantComplex2 = TimeInstantCondition(RuntimeState.END, AppliesTo.PROCESS)

    complex2 = ComplexState(timeInstantComplex1, timeInstantComplex2, Type.FOLLOWS)

    complex = ComplexState(timeInstant1, complex2, Type.FOLLOWS)

    result = condition_computer(processDataframe, "case:concept:name", complex, 'lifecycle:transition', 'concept:name')

    result_expected = [False, False, False, False, False, False, False, True]

    assert (result.values == result_expected).all()
