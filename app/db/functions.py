from datetime import datetime, date, timedelta


today_date = date.today()
initial_format = '%b%d%Y'
final_format = '%Y-%m-%d'

def formate_date(d_month: str, d_day: str) -> str:
    file_date = datetime.strptime(
        d_month + d_day + str(today_date.year), initial_format
        ).strftime(final_format)
    return str(file_date)


def yesterday(days_back: int) -> str:
    return str(today_date + timedelta(days=-days_back))
