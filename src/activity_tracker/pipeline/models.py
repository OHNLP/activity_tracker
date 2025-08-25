import pathlib

import datajoint as dj
import pandas as pd

from .. import utils
from . import get_schema_name
from .measurement import DailyMeasurement, Frailty

schema = dj.schema(get_schema_name("models"))


@schema
class Feature(dj.Imported):
    """
    A table for storing feature matrices.
    """

    definition = """
    # Feature table
    feature_id: tinyint unsigned
    ---
    feature_matrix: longblob
    feature_description="": varchar(255)
    """

    @classmethod
    def get_feature_matrix(cls, feature_id: int) -> pd.DataFrame:
        feature_matrix = (cls & f"feature_id = {feature_id}").fetch1("feature_matrix")
        return pd.DataFrame(feature_matrix["data"], columns=feature_matrix["columns"])


@schema
class Parameter(dj.Lookup):
    """
    A table for storing model parameters.
    """

    definition = """
    # Parameters table
    parameter_id: tinyint unsigned
    ---
    param_set: longblob # dictionary of parameters
    param_hash: uuid
    param_description="": varchar(255)
    """


@schema
class Model(dj.Manual):
    """
    A table for storing model info & evaluation metrics.
    """

    definition = """
    # Model table
    model_id: tinyint unsigned
    ---
    -> [nullable] Feature
    -> [nullable] Parameter
    model_name: varchar(32) # e.g., 'random_foredst', 'logistic_regression'
    model_eval: longblob # dictionary of evaluation metrics
    """
