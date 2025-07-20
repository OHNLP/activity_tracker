import datajoint as dj

from . import get_schema_name
from .subject import Subject
from .visit import Visit

# Define schema
schema = dj.schema(get_schema_name("measurement"))


@schema
class Frailty(dj.Imported):
    """
    Frailty measurement at each visit.
    """

    definition = """
    # Frailty measurement at each visit
    -> Subject
    -> Visit
    ---
    wt_loss=null: bool
    weak=null: bool
    slow=null: bool
    exhaust=null: bool
    phys_act=null: bool
    ffp_score=null: tinyint  # 0-5
    ffp_status=null: varchar(32) # "frail", "prefrail", "robust"
    ffp_status_binary=null: varchar(32) # Combine 'prefrail' and 'robust' into 'no_frail'
    """

    def make(self, key):
        pass


@schema
class DailyMeasurement(dj.Imported):
    """
    Daily measurement from the activity tracker.
    """

    definition = """
    # Daily measurement from the activity tracker.
    -> Subject
    date: date
    ---
    total_steps=null: int
    total_distance=null: float
    sedentary_minutes=null: int
    calories_bmr=null: int
    """

    def make(self, key):
        pass
