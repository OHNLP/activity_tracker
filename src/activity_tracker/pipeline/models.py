import pathlib

import datajoint as dj
import pandas as pd

from .. import utils
from . import get_schema_name
from .measurement import DailyMeasurement, Frailty
from .subject import Subject

schema = dj.schema(get_schema_name("models"))


@schema
class Feature(dj.Computed):
    """
    Feature table.
    """

    definition = """
    # Feature table
    -> feature_id: tinyint
    ---
    feature_matrix: longblob
    feature_description="": varchar(255)
    """

    def make(self, key):
        pass


@schema
class Parameter(dj.Lookup):
    """
    Parameters table.
    """

    definition = """
    # Parameters table
    parameter_id: int
    --
    param_set: longblob # dictionary of parameters
    param_hash: uuid
    param_description="": varchar(255)
    """


@schema
class Model(dj.Manual):
    """
    Model table.
    """

    definition = """
    # Model table
    model_id: int
    ---
    -> [nullable] Feature
    -> [nullable] Parameters
    model_name: varchar(32) # e.g., 'random_foredst', 'logistic_regression'
    model_eval: longblob # dictionary of evaluation metrics
    """
