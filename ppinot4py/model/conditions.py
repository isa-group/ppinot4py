#Possibly not needed
from .states import DataObjectState
import enum

class AppliesTo(enum.Enum):
    PROCESS = 1
    DATA = 2
    ACTIVITY = 3

class DataCondition():
    """
        Evaluates to true for all events whose attributes meet the condition.

        Parameters
        ----------
        condition : state or str
            The condition that is evaluated in each event.
        applies_to : AppliesTo (default DATA)
            The element of the process to which condition applies.
    """
    def __init__(self, condition, applies_to=AppliesTo.DATA):
        if isinstance(condition, str):
            condition = DataObjectState(condition)

        self.condition = condition
        self.applies_to = applies_to

    def __repr__(self):
        return f"{self.condition} - {self.applies_to}"

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
    
    applies_to : AppliesTo (default DATA)
        The element of the process to which changes_to_state applies.
    """
    
    def __init__(self, changes_to_state, applies_to=AppliesTo.DATA, activity_name=None):
        if isinstance(changes_to_state, str):
            changes_to_state = DataObjectState(changes_to_state)
        
        self.changes_to_state = changes_to_state
        self.applies_to = applies_to
        self.activity_name = activity_name
    
    def __repr__(self):
        if self.applies_to == AppliesTo.DATA:
            return f"{self.changes_to_state}"
        elif self.applies_to == AppliesTo.PROCESS:
            return f"{self.changes_to_state} - {self.applies_to}"
        else:
            return f"{self.changes_to_state} - {self.applies_to} - {self.activity_name}"




class SeriesCondition():

    def __init__(self, series):
        self.series = series
    
    def __repr__(self):
        return "%s" % (self.series)