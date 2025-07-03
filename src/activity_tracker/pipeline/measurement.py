import pathlib

import datajoint as dj
import pandas as pd

from .. import utils
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
