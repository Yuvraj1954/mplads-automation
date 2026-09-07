from datetime import date


SANCTION_DELAY_FLAGS = [
    ("SANCTION_DELAY_VERY_HIGH", "p95"),
    ("SANCTION_DELAY_HIGH", "p90"),
    ("SANCTION_DELAY_ELEVATED", "p75"),
]


RISK_FLAG_DESCRIPTIONS = {
    "SANCTION_DELAY_VERY_HIGH": {
        "flag": "SANCTION_DELAY_VERY_HIGH",
        "severity": "FLAGGED",
        "description": (
            "Sanction took much longer than typical for comparable works."
        ),
    },
    "SANCTION_DELAY_HIGH": {
        "flag": "SANCTION_DELAY_HIGH",
        "severity": "FLAGGED",
        "description": (
            "Sanction took longer than typical for comparable works."
        ),
    },
    "SANCTION_DELAY_ELEVATED": {
        "flag": "SANCTION_DELAY_ELEVATED",
        "severity": "WARNING",
        "description": (
            "Sanction took somewhat longer than typical for comparable works."
        ),
    },
    "LONG_PENDING": {
        "flag": "LONG_PENDING",
        "severity": "FLAGGED",
        "description": (
            "Completion has not been recorded and pending duration exceeds "
            "the typical completion time for comparable works."
        ),
    },
    "LOW_EXPENDITURE": {
        "flag": "LOW_EXPENDITURE",
        "severity": "WARNING",
        "description": (
            "No expenditure has been recorded despite an active sanction."
        ),
    },
    "PAYMENT_STAGNATION": {
        "flag": "PAYMENT_STAGNATION",
        "severity": "FLAGGED",
        "description": (
            "No payment activity has been recorded for over 6 months."
        ),
    },
    "COST_ANOMALY": {
        "flag": "COST_ANOMALY",
        "severity": "FLAGGED",
        "description": (
            "Sanction amount is unusually high compared to similar completed works."
        ),
    },
    "DURATION_ANOMALY": {
        "flag": "DURATION_ANOMALY",
        "severity": "FLAGGED",
        "description": (
            "Completion time is unusually long compared to similar completed works."
        ),
    },
    "OVER_EXPENDITURE": {
        "flag": "OVER_EXPENDITURE",
        "severity": "FLAGGED",
        "description": (
            "Recorded expenditure exceeds the sanctioned amount."
        ),
    },
    "POST_COMPLETION_PAYMENT": {
        "flag": "POST_COMPLETION_PAYMENT",
        "severity": "WARNING",
        "description": (
            "Payment activity was recorded after the completion date."
        ),
    },
    "SOURCE_DATA_DEFECT_NEGATIVE_DELAY": {
        "flag": "SOURCE_DATA_DEFECT_NEGATIVE_DELAY",
        "severity": "INFO",
        "description": (
            "Sanction date precedes recommendation date. "
            "This is a source data defect."
        ),
    },
}


def compute_delay_thresholds(valid_delays, p75, p90, p95):
    return {
        "delay_p75": p75,
        "delay_p90": p90,
        "delay_p95": p95,
    }


def compute_risk_flags(work, delay_thresholds, duration_p90):
    flags = []

    san_delay = work.get("sanction_delay_days")
    status = work.get("status")
    pending = work.get("pending_days")
    sanction_amount = work.get("sanction_amount") or 0
    expenditure_amount = work.get("expenditure_amount") or 0
    last_exp_date = work.get("last_expenditure_date")
    completion_date = work.get("completion_date")
    cost_status = work.get("cost_status")
    duration_status = work.get("duration_status")
    today = work.get("_reference_date", date.today())

    if san_delay is not None and san_delay < 0:
        flags.append("SOURCE_DATA_DEFECT_NEGATIVE_DELAY")

    if san_delay is not None and san_delay >= 0:
        p95 = delay_thresholds.get("delay_p95")
        p90 = delay_thresholds.get("delay_p90")
        p75 = delay_thresholds.get("delay_p75")

        if p95 is not None and san_delay > p95:
            flags.append("SANCTION_DELAY_VERY_HIGH")
        elif p90 is not None and san_delay > p90:
            flags.append("SANCTION_DELAY_HIGH")
        elif p75 is not None and san_delay > p75:
            flags.append("SANCTION_DELAY_ELEVATED")

    if (status != "Completed" and pending is not None
            and duration_p90 is not None
            and pending > duration_p90):
        flags.append("LONG_PENDING")

    if (status != "Completed"
            and sanction_amount > 0
            and work.get("expenditure_amount") is not None
            and expenditure_amount == 0
            and pending is not None
            and pending > 180):
        flags.append("LOW_EXPENDITURE")

    if (status != "Completed"
            and last_exp_date is not None
            and today is not None):
        stagnation_days = (today - last_exp_date).days
        if stagnation_days > 180:
            flags.append("PAYMENT_STAGNATION")

    if cost_status in ("VERY_HIGH", "HIGH"):
        flags.append("COST_ANOMALY")

    if (status == "Completed"
            and duration_status in ("VERY_LONG", "LONG")):
        flags.append("DURATION_ANOMALY")

    if sanction_amount > 0 and expenditure_amount > sanction_amount * 1.05:
        flags.append("OVER_EXPENDITURE")

    if (completion_date is not None
            and last_exp_date is not None
            and last_exp_date > completion_date):
        flags.append("POST_COMPLETION_PAYMENT")

    return flags


def classify_risk_level(flags):
    non_defect_flags = [f for f in flags if f != "SOURCE_DATA_DEFECT_NEGATIVE_DELAY"]

    if "OVER_EXPENDITURE" in non_defect_flags:
        return "HIGH"

    count = len(non_defect_flags)

    if count >= 3:
        return "HIGH"
    elif count == 2:
        return "MEDIUM"
    elif count == 1:
        return "LOW"
    else:
        return "NORMAL"


def describe_risk_flags(flags, work):
    result = []
    for flag in flags:
        entry = RISK_FLAG_DESCRIPTIONS.get(flag)
        if entry is None:
            entry = {
                "flag": flag,
                "severity": "WARNING",
                "description": flag.replace("_", " ").title(),
            }
        result.append({
            "flag": entry["flag"],
            "severity": entry["severity"],
            "description": _enrich_description(entry["description"], flag, work),
        })
    return result


def _enrich_description(base, flag, work):
    if flag == "SANCTION_DELAY_VERY_HIGH":
        d = work.get("sanction_delay_days")
        if d is not None:
            return f"Sanction took {d} days, exceeding the 95th percentile for comparable works."
        return base
    if flag == "SANCTION_DELAY_HIGH":
        d = work.get("sanction_delay_days")
        if d is not None:
            return f"Sanction took {d} days, exceeding the 90th percentile for comparable works."
        return base
    if flag == "SANCTION_DELAY_ELEVATED":
        d = work.get("sanction_delay_days")
        if d is not None:
            return f"Sanction took {d} days, exceeding the 75th percentile for comparable works."
        return base
    if flag == "LONG_PENDING":
        p = work.get("pending_days")
        if p is not None:
            return f"Pending for {p} days, exceeding the typical completion duration for comparable works."
        return base
    if flag == "PAYMENT_STAGNATION":
        return "No payment activity has been recorded for over 6 months."
    if flag == "LOW_EXPENDITURE":
        return "No expenditure has been recorded despite an active sanction."
    return base


def compute_positive_signals(work):
    signals = []
    rec = work.get("recommended_amount")
    san = work.get("sanction_amount")
    if (rec is not None and san is not None
            and rec > 0 and san > 0 and san == rec):
        signals.append({
            "signal": "SANCTION_MATCHES_RECOMMENDATION",
            "description": "Sanctioned amount matches the recommended amount.",
        })

    last_exp = work.get("last_expenditure_date")
    today = work.get("_reference_date")
    status = work.get("status")
    if last_exp is not None and today is not None and status != "Completed":
        days = (today - last_exp).days
        if days <= 180:
            signals.append({
                "signal": "RECENT_PAYMENT_ACTIVITY",
                "description": (
                    f"Payment activity recorded {days} days ago."
                ),
            })

    exp_pct = work.get("expenditure_percentage")
    if (exp_pct is not None and exp_pct >= 75 and exp_pct <= 100):
        signals.append({
            "signal": "EXPENDITURE_ON_TRACK",
            "description": (
                f"Expenditure utilization is {exp_pct:.0f}% of sanctioned amount."
            ),
        })

    if status == "Completed":
        signals.append({
            "signal": "COMPLETION_RECORDED",
            "description": "Work has been marked as completed.",
        })

    cost = work.get("cost_status")
    if cost == "NORMAL":
        signals.append({
            "signal": "COST_WITHIN_PEER_RANGE",
            "description": "Sanction amount is within the typical range for comparable works.",
        })

    dur = work.get("duration_status")
    if dur == "NORMAL":
        signals.append({
            "signal": "DURATION_WITHIN_PEER_RANGE",
            "description": "Completion time is within the typical range for comparable works.",
        })

    return signals
