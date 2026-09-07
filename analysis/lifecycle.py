from datetime import date


def compute_sanction_delay_days(recommendation_date, sanction_date):
    if recommendation_date is None or sanction_date is None:
        return None
    delta = (sanction_date - recommendation_date).days
    return delta


def compute_project_age_days(recommendation_date, reference_date):
    if recommendation_date is None:
        return None
    delta = (reference_date - recommendation_date).days
    return delta


def compute_execution_days(sanction_date, completion_date):
    if sanction_date is None or completion_date is None:
        return None
    if completion_date < sanction_date:
        return None
    delta = (completion_date - sanction_date).days
    return delta


def compute_pending_days(status, sanction_date, reference_date):
    if status == "Completed":
        return None
    if sanction_date is None:
        return None
    delta = (reference_date - sanction_date).days
    return delta


def compute_lifecycle(recommendation_date, sanction_date,
                      completion_date, status, reference_date):
    sanction_delay = compute_sanction_delay_days(
        recommendation_date, sanction_date
    )
    project_age = compute_project_age_days(
        recommendation_date, reference_date
    )
    execution = compute_execution_days(
        sanction_date, completion_date
    )
    pending = compute_pending_days(
        status, sanction_date, reference_date
    )
    return {
        "sanction_delay_days": sanction_delay,
        "project_age_days": project_age,
        "execution_days": execution,
        "pending_days": pending,
    }
