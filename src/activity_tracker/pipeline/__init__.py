import datajoint as dj


def get_schema_name(name) -> str:
    """Return a schema name."""
    db_prefix = dj.config["custom"]["database.prefix"]
    return db_prefix + name
