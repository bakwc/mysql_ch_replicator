from enum import Enum

class Status(Enum):
    NONE = 0
    CREATING_INITIAL_STRUCTURES = 1
    PERFORMING_INITIAL_REPLICATION = 2
    RUNNING_REALTIME_REPLICATION = 3
