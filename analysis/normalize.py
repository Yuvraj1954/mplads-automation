import re

_ACTIVITY_PATTERN = re.compile(
    r'^WS/\s*MP\d+/\d{4}-\d{4}/\d+-',
    re.IGNORECASE
)


def normalize_activity(activity_name):
    if not activity_name:
        return activity_name

    act = str(activity_name).strip()

    match = _ACTIVITY_PATTERN.match(act)
    if match:
        return act[match.end():].strip()

    return act
