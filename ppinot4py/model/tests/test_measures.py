from ppinot4py.model import *
import pytest

def test_count_text():
    c = CountMeasure("ACT == 'open'")
    assert f"{c}" == "the number of times ACT == 'open'"

def test_linear_time_text():
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'")
    assert f"{t}" == "the duration between the first time instant when ACT == 'open' and the last time instant when ACT == 'close'"

def test_linear_business_time_text():
    b = BusinessDuration("07:00", "17:00")
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'", business_duration=b)
    assert f"{t}" == "the business duration between the first time instant when ACT == 'open' and the last time instant when ACT == 'close'"

def test_cyclic_business_time_text():
    b = BusinessDuration("07:00", "17:00")
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'", time_measure_type='CYCLIC', single_instance_agg_function='AVG', business_duration=b)
    assert f"{t}" == "the average business duration between the pairs of time instants when ACT == 'open' and when ACT == 'close'"

def test_data_text():
    d = DataMeasure("AMOUNT")
    assert f"{d}" == "the last value of AMOUNT"

def test_data_text_special_activity_attribute():
    d = DataMeasure("concept:name")
    assert f"{d}" == "the last value of activity"

def test_data_text_special_timestamp_attribute():
    d = DataMeasure("time:timestamp")
    assert f"{d}" == "the last value of timestamp"

def test_data_text_with_readable_precondition():
    d = DataMeasure("concept:name", precondition="`concept:name` == 'Create Fine'")
    assert f"{d}" == 'the last value of activity when activity "Create Fine" occurs'

def test_aggregated_text():
    c = CountMeasure("ACT == 'open'")
    a = AggregatedMeasure(c, 'AVG', DataMeasure('PROJECT'))
    assert f"{a}" == "the average of the number of times ACT == 'open' grouped by the last value of PROJECT"

def test_aggregated_text_median():
    c = CountMeasure("ACT == 'open'")
    a = AggregatedMeasure(c, 'MEDIAN')
    assert f"{a}" == "the median of the number of times ACT == 'open'"

def test_aggregated_text_percentile():
    c = CountMeasure("ACT == 'open'")
    a = AggregatedMeasure(c, 'P90')
    assert f"{a}" == "the 90 percentile of the number of times ACT == 'open'"

def test_derived_text():
    c = CountMeasure("ACT == 'open'")
    t = TimeMeasure("ACT == 'open'", "ACT == 'close'", first_to=True)
    d = DerivedMeasure("c/t", {"c": c, "t": t})
    assert f"{d}" == "the function c/t where c is the number of times ACT == 'open', t is the duration between the first time instant when ACT == 'open' and the first time instant when ACT == 'close'"

def test_invalid_rolling_window():
    with pytest.raises(ValueError):
        RollingWindow(window=2)

def test_complex_state_wraps_data_object_state_strings():
    cs = ComplexState("ACT == 'A'", "ACT == 'B'", Type.FOLLOWS)

    assert isinstance(cs.first, DataObjectState)
    assert isinstance(cs.last, DataObjectState)

def test_complex_state_invalid_type_error():
    with pytest.raises(ValueError):
        ComplexState("ACT == 'A'", "ACT == 'B'", Type.LEADSTOCYCLIC)

def test_complex_state_follows_text():
    cs = ComplexState("ACT == 'A'", "ACT == 'B'", Type.FOLLOWS)
    expected = "ACT == 'B' immediately follows ACT == 'A'"

    assert str(cs) == expected
    assert repr(cs) == expected

def test_complex_state_leadsto_text():
    cs = ComplexState("ACT == 'A'", "ACT == 'B'", Type.LEADSTO)
    expected = "ACT == 'A' leads to ACT == 'B' (one match per ACT == 'A')"

    assert str(cs) == expected
    assert repr(cs) == expected

def test_data_object_state_activity_text():
    ds = DataObjectState("`concept:name` == 'A'")
    assert str(ds) == 'activity "A" occurs'

def test_data_object_state_lifecycle_text():
    ds = DataObjectState("`lifecycle:transition` == 'In Progress'")
    assert str(ds) == 'lifecycle transition "In Progress" occurs'
