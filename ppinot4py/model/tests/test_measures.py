from ppinot4py.model import *

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
