def compute_financial(recommended_amount, sanction_amount,
                      expenditure_amount, completion_amount):
    san = sanction_amount or 0

    expenditure_pct = None
    if san > 0 and expenditure_amount is not None:
        expenditure_pct = round((expenditure_amount / san) * 100, 2)

    completion_pct = None
    if san > 0 and completion_amount is not None and completion_amount > 0:
        completion_pct = round((completion_amount / san) * 100, 2)

    return {
        "recommended_amount": recommended_amount or 0,
        "sanction_amount": san,
        "expenditure_amount": expenditure_amount,
        "completion_amount": completion_amount,
        "expenditure_percentage": expenditure_pct,
        "completion_percentage": completion_pct,
    }
