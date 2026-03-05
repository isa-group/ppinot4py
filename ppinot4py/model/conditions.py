import pandas as pd

from .states import DataObjectState, ComplexState, RuntimeState
import enum

class AppliesTo(enum.Enum):
    PROCESS = 1
    DATA = 2
    ACTIVITY = 3

def _data_object_state_auto_wrap(condition):
    if condition is None:
        return None

    if isinstance(condition, str):
        condition = DataObjectState(condition)
    
    return condition

class DataCondition():
    """
        Evaluates to true for all events whose attributes meet the condition.

        Parameters
        ----------
        condition : str or bool
            The condition that is evaluated in each event.
    """
    def __init__(self, 
                 condition: str | bool):
        self.condition = condition

    def __repr__(self):
        return f"(Data Condition {self.condition})"

    def __str__(self):
        return f"an event meets the condition {self.condition}"

class TimeInstantCondition():
    """
    Evaluates to true for all events whose attribute change from
    !condition to condition.
    
    Parameters
    ----------
    changes_to_state : state or str 
        The conditions evaluates to true when there is an event that
        makes this state to become true. If it is a string, it is
        interpreted as a DataObjectState.
    
    applies_to : AppliesTo (default AppliesTo.DATA)
        The element of the process to which changes_to_state applies.

    activity_name : str (optional)
        The name of the activity to which the condition applies. 
        Only used when applies_to is AppliesTo.ACTIVITY.
    """
    
    def __init__(self, 
                 changes_to_state: str | DataObjectState | ComplexState | RuntimeState, 
                 applies_to: AppliesTo=AppliesTo.DATA, 
                 activity_name: str | None=None):
        
        if isinstance(changes_to_state, ComplexState):
            if applies_to != AppliesTo.DATA or activity_name is not None:
                raise ValueError("ComplexState is only supported with AppliesTo.DATA and activity_name=None")
        
        self.changes_to_state = _data_object_state_auto_wrap(changes_to_state)
        self.applies_to = applies_to
        self.activity_name = activity_name
    
    def __repr__(self):
        if self.applies_to == AppliesTo.DATA:
            return f"{self.changes_to_state}"
        elif self.applies_to == AppliesTo.PROCESS:
            return f"{self.changes_to_state} - {self.applies_to}"
        else:
            return f"{self.changes_to_state} - {self.applies_to} - {self.activity_name}"

    def __str__(self):
        return self.__repr__()


class SeriesCondition():

    def __init__(self, series: pd.Series):
        self.series = series
    
    def __repr__(self):
        return "%s" % (self.series)
