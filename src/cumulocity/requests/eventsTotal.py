import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm


def requestTotalEvents(year, month):
    c8y_data = readFile('c8y_data.json')
    c8y_events = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        response = MonthlyEvents(device, enforceBounds=True).requestEventCount(year, month)
        while response['count'] < 0:
            response = MonthlyEvents(device, enforceBounds=True).requestAggregatedEventCount(year, month)

        c8y_events.append({
            "deviceId": device['id'],
            "deviceType": device['type'],
            "total": {
                "count": response['count'],
                "event": response['event']
            }
        })
    return c8y_events


def requestMonthlyData(startingDate: date, lastDate: date):
    if startingDate <= lastDate:
        raise ValueError("Last date can't be before starting date")

    startingDate = startingDate.replace(day=1)
    lastDate = lastDate.replace(day=1)
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        year = currentDate.year
        month = currentDate.month

        filePath = f"events/total/{MonthlyEvents.fileName(year, month)}"
        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestTotalEvents(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
