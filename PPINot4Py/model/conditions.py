#Possibly not needed
from .states import DataObjectState
import enum

class DataCondition():
    def __init__(self, applies_to):
        self.applies_to = applies_to

    def __repr__(self):
        return "%s" % (self.applies_to)

class AppliesTo(enum.Enum):
    PROCESS = 1
    DATA = 2
class TimeInstantCondition():
    """When condition passes from !condition to condition.
    
    Parameters
    ----------
    changes_to_state : state or str 
        The conditions evaluates to true when there is an event that
        makes this state to become true. If it is a string, it is
        interpreted as a DataObjectState.
    
    applies_to : AppliesTo (default DATA)
        The element of the process to which changes_to_state applies.
    """
    
    def __init__(self, changes_to_state, applies_to=AppliesTo.DATA):
        if isinstance(changes_to_state, str):
            changes_to_state = DataObjectState(changes_to_state)
        
        self.changes_to_state = changes_to_state
        self.applies_to = applies_to
    
    def __repr__(self):
        if self.applies_to == AppliesTo.DATA:
            return f"{self.changes_to_state}"
        else:
            return f"{self.changes_to_state} - {self.applies_to}"




class SeriesCondition():

    def __init__(self, series):
        self.series = series
    
    def __repr__(self):
        return "%s" % (self.series)