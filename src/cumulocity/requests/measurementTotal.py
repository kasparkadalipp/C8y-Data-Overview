import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm


def requestMissingValues(year, month, filePath):
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}

    fileContents = readFile(filePath)
    devicesWithMissingValues = sum([device['total']['count'] < 0 for device in fileContents])

    if not devicesWithMissingValues:
        return []

    c8y_measurements = []
    description = f"{calendar.month_abbr[month]} {year} - missing values"
    with tqdm(total=devicesWithMissingValues, desc=description, bar_format=tqdmFormat) as progressBar:
        for savedMeasurement in readFile(filePath):
            if savedMeasurement['total']['count'] >= 0:
                c8y_measurements.append(savedMeasurement)
                continue

            progressBar.update(1)
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
    c8y_data = readFile('c8y_data.json')
    c8y_measurements = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        response = MonthlyMeasurements(device, enforceBounds=True).requestMeasurementCount(year, month)
        while response['count'] < 0:
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


def requestMonthlyData(startingDate: date, lastDate: date):
    if startingDate <= lastDate:
        raise ValueError("Last date can't be before starting date")

    startingDate = startingDate.replace(day=1)
    lastDate = lastDate.replace(day=1)
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        year = currentDate.year
        month = currentDate.month

        filePath = f"measurements/total/{MonthlyMeasurements.fileName(year, month)}"
        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestTotal(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
