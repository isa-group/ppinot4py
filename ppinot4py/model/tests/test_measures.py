from ppinot4py.model import *
from datetime import time
from ppinot4py.computers import *
import holidays as pyholidays
import numpy as np
import calendar

def test_count_text():
    c = CountMeasure("ACT == 'open'")
    assert f"{c}" == "the number of times ACT == 'open'"

def test_linear_time_text():
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'")
    assert f"{t}" == "the duration between the first time instant when ACT == 'open' and the last time instant when ACT == 'close'"

def test_data_text():
    d = DataMeasure("AMOUNT")
    assert f"{d}" == "the last value of AMOUNT"

def test_aggregated_text():
    c = CountMeasure("ACT == 'open'")
    a = AggregatedMeasure(c, 'AVG', DataMeasure('PROJECT'))
    assert f"{a}" == "the average of the number of times ACT == 'open' grouped by the last value of PROJECT"

def test_derived_text():
    c = CountMeasure("ACT == 'open'")
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'", first_to=True)
    d = DerivedMeasure("c/t", {"c": c, "t": t})
    assert f"{d}" == "the function c/t where c is the number of times ACT == 'open', t is the duration between the first time instant when ACT == 'open' and the first time instant when ACT == 'close'"

def test_Business_Duration_text():

    newWeekEnd = []
    b = BusinessDuration(business_start = time(7,0,0),
    business_end = time(17,0,0),
    weekend_list = [5,6],
    holiday_list = pyholidays.ES(prov ='AN', years=2018),
    unit_hour = 'sec')

    daysInText = BusinessDuration.day_of_the_week(b.weekend_list)
    listOfNamesInYear = BusinessDuration.holidays_in_the_year(b.holiday_list)

    assert f"{b}" == f"The business time starts at {b.business_start}, ends at {b.business_end}, the weekend is defined in the days {daysInText}, the name of the holidays days are: {listOfNamesInYear} and the unit of time used is {b.unit_hour}"

def test_cyclic_time_text():

    t = TimeMeasure(
        from_condition='`lifecycle:transition` == "In Progress"', 
        to_condition='`lifecycle:transition` == "Awaiting Assignment"', 
        single_instance_agg_function='SUM',
        time_measure_type='CYCLIC')

    precon_text = f" if {t.precondition}" if t.precondition is not None else ""

    assert f"{t}" == f"the sum duration between the pairs of time instants when {t.from_condition} and when {t.to_condition}{precon_text}"

def test_aggregated_filter_to_apply_text():
    c = CountMeasure("ACT == 'open'")
    d = CountMeasure("ACT == 'closed'")
    aggregatedMeasure = AggregatedMeasure(
        base_measure=c, 
        single_instance_agg_function='AVG',
        filter_to_apply=d)

    assert f"{aggregatedMeasure}" == "the average of the number of times ACT == 'open' filtered by the number of times ACT == 'closed'"

def test_data_first_text():
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'")
    d = DataMeasure(
        data_content_selection="lifecycle:transition", 
        first=True)
    assert f"{d}" == "the first value of lifecycle:transition"

def test_aggregated_text_lower_case():
    a = AggregatedMeasure(CountMeasure("concept:name=='Appeal to Judge'"), 'avg')
    assert f"{a}" == "the average of the number of times concept:name=='Appeal to Judge'"


def test_aggregated_text_time_instant_condition():
    b = AggregatedMeasure(CountMeasure(TimeInstantCondition(RuntimeState.START, AppliesTo.ACTIVITY, "Appeal to Judge")), 'avg')
    assert f"{b}" == "the average of the number of times when starts the ACTIVITY with name Appeal to Judge"


