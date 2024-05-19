from datetime import date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm
import calendar
from dateutil.parser import parse

load_dotenv('../.env')
c8y_data = readFile('telia/c8y_data.json')
deviceIdMapping = {device['id']: device for device in c8y_data}


def requestMissingValues(year, month, filePath):
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


print(f'Oldest event {min([parse(d['oldestEvent']['time']).date() for d in c8y_data if d['oldestEvent']])}')
print(f'Latest event {max([parse(d['latestEvent']['time']).date() for d in c8y_data if d['latestEvent']])}')

startingDate = date(2016, 11, 1)
lastDate = date(2024, 3, 1)

currentDate = lastDate
while startingDate <= currentDate <= lastDate:
    year = currentDate.year
    month = currentDate.month

    filePath = f"telia/events/total/{MonthlyEvents.fileName(year, month)}"
    if pathExists(filePath):
        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath, overwrite=True)
        else:
            print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestTotalEvents(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
