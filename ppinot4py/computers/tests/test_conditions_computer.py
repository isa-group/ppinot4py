import datetime
import pandas as pd
import pytest
from ppinot4py.model import RuntimeState, TimeInstantCondition, AppliesTo, ComplexState, Type, DataObjectState
from ppinot4py.computers import condition_computer
from ppinot4py.computers.conditions_computer import _complex_state_condition_resolve


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
    result = condition_computer(dataframeLinear, "case:concept:name", cond, 'lifecycle:transition', 'concept:name')

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
    result = condition_computer(dataframeLinear, "case:concept:name", cond, 'lifecycle:transition', 'concept:name')

    result_expected = [False, False, False, False, False, False, True, True]

    assert (result.values == result_expected).all()


def test_time_instant_activity_start():

        IdCase1 = '1-364285768'
        IdCase2 = '1-364285769'

        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time1b = datetime.datetime(2010, 3, 31, 17, 32, 48)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)

        data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
                'concept:name': ['Queued', 'Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
                'time:timestamp': [time1, time1b, time2, time3, time4, time5],
                'lifecycle:transition': ['Assigned', 'Assigned', 'Awaiting Assignment','Completed', 'Assigned','Completed']}
        
        dataframeLinear = pd.DataFrame(data)

        precondition = TimeInstantCondition(RuntimeState.START, AppliesTo.ACTIVITY, "'Queued'")
        result = condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')
        
        result_expected = [True, False, False, False, True, False]

        assert (result.values == result_expected).all()


def test_time_instant_activity_end():

        IdCase1 = '1-364285768'
        IdCase2 = '1-364285769'

        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)

        data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase2, IdCase2], 
                'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
                'time:timestamp': [time1, time2, time3, time4, time5],
                'lifecycle:transition': ['Assigned', 'Awaiting Assignment','Completed', 'Assigned','Completed']}
        
        dataframeLinear = pd.DataFrame(data)

        precondition = TimeInstantCondition(RuntimeState.END, AppliesTo.ACTIVITY, "'Not queued'")
        result = condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')
        
        result_expected = [False, False, True, False, True]

        assert (result.values == result_expected).all()


def test_time_instant_activity_custom():

        IdCase1 = '1-364285768'
        IdCase2 = '1-364285769'

        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
        time6 = datetime.datetime(2013, 5, 1, 16, 44, 7)

        data = {'case:concept:name': [IdCase1, IdCase1, IdCase1, IdCase2, IdCase2, IdCase2], 
                'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Queued', 'Not queued'],
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['Assigned', 'Awaiting Assignment','Completed', 'Assigned', 'Awaiting Assignment', 'Completed']}
        
        dataframeLinear = pd.DataFrame(data)

        precondition = TimeInstantCondition("'Awaiting Assignment'", AppliesTo.ACTIVITY, "'Queued'")
        result = condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')
        
        result_expected = [False, True, False, False, True, False]

        assert (result.values == result_expected).all()

def test_time_instant_process_activity_no_lifecycle_END():

        IdCase1 = '1-364285768'
        IdCase2 = '1-364285769'
        IdCase3 = '1-364285770'
        IdCase4 = '1-364285771'
        IdCase5 = '1-364285772'

        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)

        data = {'case:concept:name': [IdCase1, IdCase2, IdCase3, IdCase4, IdCase5], 
                'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
                'time:timestamp': [time1, time2, time3, time4, time5]
        }

        dataframeLinear = pd.DataFrame(data)

        precondition = TimeInstantCondition(RuntimeState.END, AppliesTo.ACTIVITY, "'Queued'")
        result = condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')

        result_expected = [True, True, False, True, False]

        assert (result.values == result_expected).all()

def test_time_instant_process_activity_no_lifecycle_START_error():

        IdCase1 = '1-364285768'
        IdCase2 = '1-364285769'
        IdCase3 = '1-364285770'
        IdCase4 = '1-364285771'
        IdCase5 = '1-364285772'

        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)

        data = {'case:concept:name': [IdCase1, IdCase2, IdCase3, IdCase4, IdCase5], 
                'concept:name': ['Queued', 'Queued', 'Not queued', 'Queued', 'Not queued'],
                'time:timestamp': [time1, time2, time3, time4, time5]
        }

        dataframeLinear = pd.DataFrame(data)

        precondition = TimeInstantCondition(RuntimeState.START, AppliesTo.ACTIVITY, "'Queued'")

        with pytest.raises(ValueError):
                condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')

def test_time_instant_complex_state_eventually():
        id_case = '1-1'

        data = {'case:concept:name': [id_case, id_case, id_case, id_case, id_case, id_case],
                'concept:name': ['A', 'C', 'B', 'A', 'B', 'B']}

        dataframeLinear = pd.DataFrame(data)

        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last=DataObjectState("`concept:name` == 'B'"),
            state_type=Type.LEADSTO
        )
        precondition = TimeInstantCondition(complex_state, AppliesTo.DATA)
        result = condition_computer(dataframeLinear, "case:concept:name", precondition, 'concept:name', 'lifecycle:transition')

        result_expected = [False, False, True, False, True, False]
        assert (result.values == result_expected).all()

def test_complex_state_raw_leadsto():
        id_case = '1-1'

        data = {'case:concept:name': [id_case, id_case, id_case, id_case, id_case, id_case],
                'concept:name': ['A', 'C', 'B', 'A', 'B', 'B']}

        dataframeLinear = pd.DataFrame(data)
        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last=DataObjectState("`concept:name` == 'B'"),
            state_type=Type.LEADSTO
        )

        raw_result = _complex_state_condition_resolve(dataframeLinear, "case:concept:name", complex_state)

        assert (raw_result.values == [False, False, True, False, True, False]).all()

def test_complex_state_raw_leadsto_requires_new_first_after_match():
        id_case = '1-1'

        data = {'case:concept:name': [id_case, id_case, id_case, id_case, id_case, id_case, id_case, id_case],
                'concept:name': ['A', 'C', 'B', 'C', 'B', 'A', 'C', 'B']}

        dataframeLinear = pd.DataFrame(data)

        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last="`concept:name` == 'B'",
            state_type=Type.LEADSTO
        )
        raw_result = _complex_state_condition_resolve(dataframeLinear, "case:concept:name", complex_state)

        assert (raw_result.values == [False, False, True, False, False, False, False, True]).all()

def test_complex_state_raw_leadsto_requires_new_first_with_intermediate_events():
        id_case = '1-1'

        data = {'case:concept:name': [id_case, id_case, id_case, id_case, id_case, id_case],
                'concept:name': ['A', 'A', 'C', 'B', 'C', 'B']}

        dataframeLinear = pd.DataFrame(data)

        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last="`concept:name` == 'B'",
            state_type=Type.LEADSTO
        )
        raw_result = _complex_state_condition_resolve(dataframeLinear, "case:concept:name", complex_state)

        assert (raw_result.values == [False, False, False, True, False, False]).all()

def test_complex_state_raw_follows_standard_sequence():
        id_case = '1-1'

        data = {'case:concept:name': [id_case, id_case, id_case, id_case, id_case, id_case],
                'concept:name': ['A', 'C', 'B', 'A', 'B', 'B']}

        dataframeLinear = pd.DataFrame(data)
        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last="`concept:name` == 'B'",
            state_type=Type.FOLLOWS
        )

        raw_result = _complex_state_condition_resolve(dataframeLinear, "case:concept:name", complex_state)

        assert (raw_result.values == [False, False, False, False, True, False]).all()

def test_time_instant_complex_state_invalid_applies_to_error():
        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last="`concept:name` == 'B'",
            state_type=Type.FOLLOWS
        )

        with pytest.raises(ValueError):
            TimeInstantCondition(complex_state, AppliesTo.ACTIVITY, "'A'")

def test_time_instant_complex_state_activity_name_not_allowed_error():
        complex_state = ComplexState(
            first="`concept:name` == 'A'",
            last="`concept:name` == 'B'",
            state_type=Type.LEADSTO
        )

        with pytest.raises(ValueError):
            TimeInstantCondition(complex_state, AppliesTo.DATA, "'A'")
