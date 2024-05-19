from datetime import date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm
import calendar

load_dotenv('../.env')
c8y_data = readFile('example/c8y_data.json')
deviceIdMapping = {device['id']: device for device in c8y_data}


def requestMissingValues(year, month, filePath):
    c8y_measurements = []
    fileContents = readFile(filePath)

    if all([device['total']['count'] >= 0 for device in fileContents]):
        return []

    for savedMeasurement in tqdm(readFile(filePath), desc=f"{calendar.month_abbr[month]} {year}",
                                 bar_format=tqdmFormat):
        if savedMeasurement['total']['count'] >= 0:
            c8y_measurements.append(savedMeasurement)
            continue

        device = deviceIdMapping[savedMeasurement['deviceId']]

        response = MonthlyMeasurements(device, enforceBounds=True).requestAggregatedMeasurementCount(year, month)
        c8y_measurements.append({
            "deviceId": device['id'],
            "deviceType": device['type'],
            "total": {
                "count": response['count'],
                "measurement": response['measurement']
            }
        })
    return c8y_measurements


def requestTotal(year, month):
    c8y_measurements = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        response = MonthlyMeasurements(device, enforceBounds=True).requestMeasurementCount(year, month)
        c8y_measurements.append({
            "deviceId": device['id'],
            "deviceType": device['type'],
            "total": {
                "count": response['count'],
                "measurement": response['measurement']
            }
        })
    return c8y_measurements


# print(f'Oldest measurement {min([parse(d['oldestMeasurement']['time']).date() for d in c8y_data if d['oldestMeasurement']])}')
# print(f'Latest measurement {max([parse(d['latestMeasurement']['time']).date() for d in c8y_data if d['latestMeasurement']])}')

startingDate = date(2014, 7, 1)
lastDate = date(2024, 3, 1)

currentDate = lastDate
while startingDate <= currentDate <= lastDate:
    year = currentDate.year
    month = currentDate.month

    filePath = f"telia/measurements/total/{MonthlyMeasurements.fileName(year, month)}"
    if pathExists(filePath):
        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath, overwrite=True)
        else:
            print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestTotal(year, month)
        saveToFile(data, filePath, overwrite=False)

    currentDate -= relativedelta(months=1)
