import datajoint as dj

from . import get_schema_name
from .subject import Subject

# Define schema
schema = dj.schema(get_schema_name("visit"))


@schema
class Visit(dj.Manual):
    """
    Study visit table.
    """

    definition = """
    # Study visits
    -> Subject
    visit_id: tinyint        # Visit number (1-5)
    ---
    date=null: date          # Visit date
    """
