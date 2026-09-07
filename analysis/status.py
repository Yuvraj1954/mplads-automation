from analysis.models import WorkStatus


def classify_status(recommendation_date, sanction_date, completion_date):
    if completion_date is not None:
        return WorkStatus.COMPLETED.value

    if sanction_date is not None:
        return WorkStatus.IN_PROGRESS.value

    if recommendation_date is not None:
        return WorkStatus.RECOMMENDED.value

    return WorkStatus.UNKNOWN.value
