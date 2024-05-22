import calendar
from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm




def requestMissingValues(year, month, filePath):
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}
    c8y_measurements = []
    fileContents = readFile(filePath)

    if all([device['total']['count'] >= 0 for device in fileContents]):
        return []

    for savedMeasurement in tqdm(readFile(filePath), desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        if savedMeasurement['total']['count'] >= 0:
            c8y_measurements.append(savedMeasurement)
            continue

        device = deviceIdMapping[savedMeasurement['deviceId']]

        response = MonthlyEvents(device, enforceBounds=True).requestAggregatedEventCount(year, month)
        c8y_measurements.append({
            "deviceId": device['id'],
            "deviceType": device['type'],
            "total": {
                "count": response['count'],
                "event": response['event']
            }
        })
    return c8y_measurements


def requestTotalEvents(year, month):
    c8y_data = readFile('c8y_data.json')
    c8y_events = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        response = MonthlyEvents(device, enforceBounds=True).requestEventCount(year, month)
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
        fileExists = pathExists(filePath)

        if not fileExists:
            data = requestTotalEvents(year, month)
            saveToFile(data, filePath)

        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath)
        elif fileExists:
            print(f"{calendar.month_abbr[month]} {year} - skipped")

        currentDate -= relativedelta(months=1)
