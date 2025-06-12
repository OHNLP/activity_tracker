import re


def normalize_subject_id(subject_id: str) -> str:
    """Normalize subject ID by padding the number with zeros.

    Args:
        subject_id: Subject ID string (e.g. "MDE1" or "MDE01")

    Returns:
        Normalized subject ID with padded zeros (e.g. "MDE001")
    """
    return re.sub(r"MDE(\d+)", lambda m: f"MDE{int(m.group(1)):03d}", subject_id)
