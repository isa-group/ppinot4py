from __future__ import annotations
import enum
from typing import ClassVar

class DataObjectState():
    """Basic state of a condition.
    
    Args:    
            - name: String representation of a condition
    """
    
    def __init__(self, name):
        self.name = name
        
    def __repr__(self):
        return "%s" % (self.name)

class ComplexState():    
    def __init__(self, first, last, state_type):
        self.first = first
        self.last = last
        self.type = state_type


class RuntimeState:
    START: ClassVar["RuntimeState"]
    END: ClassVar["RuntimeState"]

    def __init__(self, state: str):
        if not isinstance(state, str) or not state.strip():
            raise ValueError("state must be a non-empty string")
        self.state = state.strip()

    def __repr__(self) -> str:
        return self.state

    def __str__(self) -> str:
        return self.state

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RuntimeState) and self.state == other.state

    def __hash__(self) -> int:
        return hash(self.state)

RuntimeState.START = RuntimeState("START")
RuntimeState.END = RuntimeState("END")


class Type(enum.Enum):
    FOLLOWS = 1
    LEADSTO = 2
    LEADSTOCYCLIC = 3