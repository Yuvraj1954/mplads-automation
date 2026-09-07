from datetime import date


def compute_days_since_last_expenditure(last_expenditure_date,
                                         reference_date):
    if last_expenditure_date is None or reference_date is None:
        return None
    return (reference_date - last_expenditure_date).days


def compute_financial_profile(expenditure_percentage, status):
    if status == "Recommended":
        return "UNKNOWN"
    if status == "Completed":
        if expenditure_percentage is not None:
            if expenditure_percentage >= 90:
                return "STRONG"
            elif expenditure_percentage >= 50:
                return "ADEQUATE"
            else:
                return "LOW"
        return "UNKNOWN"
    if expenditure_percentage is not None:
        if expenditure_percentage >= 90:
            return "STRONG"
        elif expenditure_percentage >= 50:
            return "ADEQUATE"
        else:
            return "LOW"
    return "UNKNOWN"


def compute_timeline_profile(status, pending_days, duration_p90,
                             sanction_delay_days):
    if status == "Completed":
        if sanction_delay_days is not None and sanction_delay_days < 0:
            return "UNKNOWN"
        return "ON_TRACK"
    if status == "Recommended":
        return "UNKNOWN"
    if pending_days is None:
        return "UNKNOWN"
    if pending_days < 0:
        return "UNKNOWN"
    threshold = duration_p90 if duration_p90 is not None else 365
    if pending_days > threshold * 1.5:
        return "SEVERELY_DELAYED"
    if pending_days > threshold:
        return "DELAYED"
    return "ON_TRACK"


def compute_payment_activity_profile(last_expenditure_date,
                                     reference_date, status):
    if status == "Completed":
        return "NO_DATA"
    if last_expenditure_date is None or reference_date is None:
        return "NO_DATA"
    days_since = (reference_date - last_expenditure_date).days
    if days_since < 0:
        return "NO_DATA"
    if days_since <= 180:
        return "ACTIVE"
    if days_since <= 365:
        return "SLOWING"
    return "STALLED"
