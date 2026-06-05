from datetime import date, timedelta


def months_between(start: date, end: date) -> list[date]:
    if start > end:
        start, end = end, start

    results = []
    year, month = start.year, start.month

    while (year, month) <= (end.year, end.month):
        results.append(date(year, month, 1))

        month += 1
        if month > 12:
            month = 1
            year += 1

    return results


def days_between(start: date, end: date) -> list[date]:
    if start > end:
        start, end = end, start

    results = []
    current = start

    while current <= end:
        results.append(current)
        current += timedelta(days=1)

    return results
